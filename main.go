package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// 定义Prometheus指标
var (
	imagePullFailureGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "k8s_pod_image_pull_failure_total",
			Help: "Number of pods with image pull failures categorized by node and reason",
		},
		[]string{"node", "registry", "reason"},
	)
	imagePullFailureAlertCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "k8s_pod_image_pull_failure_alerts_total",
			Help: "Total number of image pull failure alerts triggered, by node and reason",
		},
		[]string{"node", "registry", "reason"},
	)
	imagePullSlowAlertCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "k8s_pod_image_pull_slow_alerts_total",
			Help: "Total number of image pull slow alerts triggered (>=5m), by node and registry",
		},
		[]string{"node", "registry", "reason"},
	)
)

// podInfo 包含失败原因、节点信息及锁
type podInfo struct {
	mu       sync.Mutex
	reasons  map[string]failureInfo // key: reason, value: failureInfo
	nodeName string
}

type failureInfo struct {
	registry string
}

type alertCount struct {
	count atomic.Int64
}

// slowPullTimers 存储 image pull 定时器
var slowPullTimers sync.Map // key:string -> *time.Timer

var (
	podFailures sync.Map // key namespace/pod -> *podInfo
	alertCounts sync.Map // key node/registry/reason -> *alertCount
	clientset   *kubernetes.Clientset
)

// 为不同失败原因预定义正则表达式，用于根据错误信息做归类
var (
	reImageNotFound = regexp.MustCompile(`(?i)not found|manifest unknown|repository does not exist`)
	reProxyError    = regexp.MustCompile(`(?i)proxyconnect|proxy error`)
	reUnauthorized  = regexp.MustCompile(`(?i)unauthorized|authentication required`)
	reTLS           = regexp.MustCompile(`(?i)tls handshake`)
)

func onPodAddOrUpdate(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}

	podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
	log.Printf(
		"[PodEvent] %s phase=%s uid=%s node=%s containers=%d",
		podKey,
		pod.Status.Phase,
		string(pod.UID),
		pod.Spec.NodeName,
		len(pod.Status.ContainerStatuses),
	)

	checkSlowPull := func(ns, podName string, cs corev1.ContainerStatus, image string) {
		key := fmt.Sprintf("%s/%s/%s", ns, podName, cs.Name)

		if cs.ContainerID == "" && cs.State.Waiting != nil && isPublicRegistry(image) {
			// 使用 time.AfterFunc 启动定时器
			timer := time.AfterFunc(5*time.Minute, func() {
				// 5 分钟后再核实一次状态
				p2, err := clientset.CoreV1().
					Pods(ns).
					Get(context.Background(), podName, metav1.GetOptions{})
				if err != nil {
					log.Printf("[SlowPull] 获取 Pod %s/%s 失败: %v", ns, podName, err)
					return
				}

				for _, newCs := range p2.Status.ContainerStatuses {
					if newCs.Name == cs.Name && newCs.ContainerID == "" {
						registry := parseRegistry(image)
						nodeName := getNodeName(p2)
						imagePullSlowAlertCounter.WithLabelValues(nodeName, registry, "slow_pull").
							Inc()
						log.Printf("[SlowPullAlert] %s/%s container=%s node=%s registry=%s",
							ns, podName, cs.Name, nodeName, registry)
					}
				}

				slowPullTimers.Delete(key)
			})

			actual, loaded := slowPullTimers.LoadOrStore(key, timer)
			if loaded {
				// 已有定时器在跑，停止新创建的
				timer.Stop()

				_ = actual
			}
		} else {
			// 拉取成功或状态变化，取消并删除定时器
			if val, exists := slowPullTimers.LoadAndDelete(key); exists {
				if t, ok := val.(*time.Timer); ok {
					t.Stop()
				}
			}
		}
	}

	// 遍历 InitContainerStatuses + ContainerStatuses
	for _, cs := range pod.Status.InitContainerStatuses {
		checkSlowPull(pod.Namespace, pod.Name, cs, cs.Image)
	}

	for _, cs := range pod.Status.ContainerStatuses {
		checkSlowPull(pod.Namespace, pod.Name, cs, cs.Image)
	}

	reasons := analyzePodImagePullErrors(pod)
	if pod.Status.Phase == corev1.PodPending && len(pod.Status.ContainerStatuses) == 0 {
		// 对于 sandbox 创建失败，使用默认 registry
		reasons["sandbox_create_failure"] = failureInfo{registry: "unknown"}
	}

	piVal, _ := podFailures.LoadOrStore(podKey, &podInfo{
		reasons:  make(map[string]failureInfo),
		nodeName: getNodeName(pod),
	})

	pi, ok := piVal.(*podInfo)
	if !ok {
		log.Printf("[onPodAddOrUpdate] 无法解析已添加对象类型: %T", piVal)
		return
	}

	pi.mu.Lock()
	defer pi.mu.Unlock()

	// 更新节点信息
	pi.nodeName = getNodeName(pod)

	updateReasons(pi, reasons, pod)
}

func onPodDelete(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			log.Printf("[onPodDelete] 无法解析已删除对象类型: %T", obj)
			return
		}

		pod, ok = tombstone.Obj.(*corev1.Pod)
		if !ok {
			log.Printf("[onPodDelete] Tombstone 对象无法转换: %T", tombstone.Obj)
			return
		}
	}

	key := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)

	reasonsVal, loaded := podFailures.LoadAndDelete(key)
	if loaded {
		pi := reasonsVal.(*podInfo)

		pi.mu.Lock()
		defer pi.mu.Unlock()

		for reason, info := range pi.reasons {
			imagePullFailureGauge.WithLabelValues(pi.nodeName, info.registry, reason).Dec()
		}
	}

	prefix := key + "/"
	slowPullTimers.Range(func(k, v any) bool {
		sk := k.(string)
		if strings.HasPrefix(sk, prefix) {
			if t, ok := v.(*time.Timer); ok {
				t.Stop()
			}

			slowPullTimers.Delete(sk)
		}

		return true
	})
}

func analyzePodImagePullErrors(pod *corev1.Pod) map[string]failureInfo {
	reasons := make(map[string]failureInfo)

	checkContainerStatuses := func(statuses []corev1.ContainerStatus) {
		for _, cs := range statuses {
			if cs.State.Waiting != nil && isImagePullFailureReason(cs.State.Waiting.Reason) {
				classified := classifyFailureReason(
					cs.State.Waiting.Reason,
					cs.State.Waiting.Message,
				)
				registry := parseRegistry(cs.Image)
				reasons[classified] = failureInfo{registry: registry}
			}
		}
	}

	checkContainerStatuses(pod.Status.InitContainerStatuses)
	checkContainerStatuses(pod.Status.ContainerStatuses)

	return reasons
}

func getNodeName(pod *corev1.Pod) string {
	if pod.Spec.NodeName != "" {
		return pod.Spec.NodeName
	}
	return "unscheduled"
}

func isPublicRegistry(image string) bool {
	return strings.HasPrefix(image, "docker.io/") ||
		strings.HasPrefix(image, "gcr.io/") ||
		strings.HasPrefix(image, "ghcr.io/") ||
		strings.HasPrefix(image, "k8s.gcr.io/") ||
		strings.HasPrefix(image, "quay.io/") ||
		strings.HasPrefix(image, "registry.k8s.io/") ||
		(strings.HasPrefix(image, "registry.") && strings.Contains(image, ".aliyuncs.com/")) ||
		(strings.HasPrefix(image, "hub.") && strings.Contains(image, ".sealos.run/"))
}

func parseRegistry(image string) string {
	if image == "" {
		return "unknown"
	}

	parts := strings.Split(image, "/")
	if len(parts) > 1 && strings.Contains(parts[0], ".") {
		return parts[0]
	}

	return "docker.io"
}

func isImagePullFailureReason(reason string) bool {
	switch reason {
	case "ErrImagePull", "ImagePullBackOff", "Cancelled", "RegistryUnavailable":
		return true
	default:
		return false
	}
}

func classifyFailureReason(reason, message string) string {
	lowMsg := strings.ToLower(message)
	switch strings.ToLower(reason) {
	case "errimagepull", "imagepullbackoff":
		if reImageNotFound.MatchString(lowMsg) {
			return "image_not_found"
		}

		if reProxyError.MatchString(lowMsg) {
			return "proxy_error"
		}

		if reUnauthorized.MatchString(lowMsg) {
			return "unauthorized"
		}

		if reTLS.MatchString(lowMsg) {
			return "tls_handshake_error"
		}
		// 其他情况
		log.Printf("[Classify] 未知错误分类 reason=%s message=%s", reason, message)

		return "unknown_error"
	default:
		return strings.ToLower(reason)
	}
}

func updateReasons(pi *podInfo, reasons map[string]failureInfo, pod *corev1.Pod) {
	// 删除旧的原因
	for reason, oldInfo := range pi.reasons {
		if _, found := reasons[reason]; !found {
			imagePullFailureGauge.WithLabelValues(pi.nodeName, oldInfo.registry, reason).Dec()
			delete(pi.reasons, reason)
		}
	}

	// 添加新的原因
	for reason, info := range reasons {
		if _, found := pi.reasons[reason]; !found {
			imagePullFailureGauge.WithLabelValues(pi.nodeName, info.registry, reason).Inc()
			imagePullFailureAlertCounter.WithLabelValues(pi.nodeName, info.registry, reason).Inc()

			countKey := fmt.Sprintf("%s/%s/%s", pi.nodeName, info.registry, reason)
			acVal, _ := alertCounts.LoadOrStore(countKey, &alertCount{})
			ac := acVal.(*alertCount)
			newCount := ac.count.Add(1)
			log.Printf("[AlertCounter] #%d %s node=%s registry=%s reason=%s",
				newCount, pod.Name, pi.nodeName, info.registry, reason)

			pi.reasons[reason] = info
		}
	}
}

func main() {
	// 注册 Prometheus 指标
	prometheus.MustRegister(imagePullFailureGauge)
	prometheus.MustRegister(imagePullFailureAlertCounter)
	prometheus.MustRegister(imagePullSlowAlertCounter)

	// 创建 in-cluster 配置
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Error creating in-cluster config: %v", err)
	}

	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error creating Kubernetes clientset: %v", err)
	}

	// 创建 informer
	factory := informers.NewSharedInformerFactory(clientset, 0)
	podInformer := factory.Core().V1().Pods().Informer()
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onPodAddOrUpdate,
		UpdateFunc: func(old, new any) { onPodAddOrUpdate(new) },
		DeleteFunc: onPodDelete,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go podInformer.Run(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced) {
		log.Fatalf("Timed out waiting for caches to sync")
	}

	// HTTP server 和优雅停机
	srv := &http.Server{
		Addr:    ":8080",
		Handler: promhttp.Handler(),
	}
	go func() {
		log.Println("Starting metrics server at :8080")

		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Metrics server error: %v", err)
		}
	}()

	// 捕获信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutdown signal received, exiting...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error during server shutdown: %v", err)
	}

	log.Println("Server gracefully stopped")
}
