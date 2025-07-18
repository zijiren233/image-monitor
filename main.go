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
			Help: "Number of pods with image pull failures categorized by exported_namespace, exported_pod, node, image and reason",
		},
		[]string{"exported_namespace", "exported_pod", "node", "registry", "image", "reason"},
	)
	imagePullFailureAlertCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "k8s_pod_image_pull_failure_alerts_total",
			Help: "Total number of image pull failure alerts triggered, by exported_namespace, exported_pod, node, image and reason",
		},
		[]string{"exported_namespace", "exported_pod", "node", "registry", "image", "reason"},
	)
	// 改为 Gauge 类型，可以进行 Inc 和 Dec 操作
	imagePullSlowAlertGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "k8s_pod_image_pull_slow_total",
			Help: "Number of pods with slow image pull (>=5m), by exported_namespace, exported_pod, node, registry and image",
		},
		[]string{"exported_namespace", "exported_pod", "node", "registry", "image"},
	)
	// 保留 Counter 用于记录慢拉取告警的累计次数
	imagePullSlowAlertCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "k8s_pod_image_pull_slow_alerts_total",
			Help: "Total number of image pull slow alerts triggered (>=5m), by exported_namespace, exported_pod, node, registry and image",
		},
		[]string{"exported_namespace", "exported_pod", "node", "registry", "image"},
	)
)

type reason = string

const (
	ReasonImageNotFound reason = "image_not_found"
	ReasonProxyError    reason = "proxy_error"
	ReasonUnauthorized  reason = "unauthorized"
	ReasonTLSHandshake  reason = "tls_handshake_error"
	ReasonBackOff       reason = "back_off_pulling_image"
	ReasonUnknown       reason = "unknown"
)

// podInfo 包含失败原因、节点信息及锁
type podInfo struct {
	mu        sync.Mutex
	reasons   map[string]failureInfo // key: container name, value: failureInfo with node info
	namespace string
	podName   string
}

// failureInfo 现在包含节点信息和镜像信息，确保 Dec 操作在正确的节点上执行
type failureInfo struct {
	registry string // 镜像仓库
	nodeName string // 节点信息
	image    string // 镜像信息
	reason   reason // 失败原因
}

// slowPullInfo 记录慢拉取的信息
type slowPullInfo struct {
	namespace string
	podName   string
	nodeName  string
	registry  string
	image     string
}

type alertCount struct {
	count atomic.Int64
}

// slowPullTimers 存储 image pull 定时器
var slowPullTimers sync.Map // key:string -> *time.Timer

// slowPullTracking 跟踪当前的慢拉取状态
var slowPullTracking sync.Map // key: namespace/pod/container -> slowPullInfo

var (
	podFailures sync.Map // key namespace/pod -> *podInfo
	alertCounts sync.Map // key exported_namespace/exported_pod/node/registry/image/reason -> *alertCount
	clientset   *kubernetes.Clientset
)

// 为不同失败原因预定义正则表达式，用于根据错误信息做归类
var (
	reImageNotFound = regexp.MustCompile(
		`(?i)not found|NotFound|manifest unknown|repository does not exist`,
	)
	reProxyError   = regexp.MustCompile(`(?i)proxyconnect|proxy error`)
	reUnauthorized = regexp.MustCompile(
		`(?i)unauthorized|authentication require|failed to authorize|authorization failed`,
	)
	reTLS = regexp.MustCompile(`(?i)tls handshake`)
)

// isBackOffPullingImage 检查是否为 back-off pulling image 状态
func isBackOffPullingImage(reason, message string) bool {
	if strings.ToLower(reason) == "imagepullbackoff" {
		return true
	}

	if strings.Contains(strings.ToLower(message), "back-off pulling image") {
		return true
	}

	return false
}

func newCheckSlowPullHandler(
	slowPullTimerKey, ns, podName string,
	cs corev1.ContainerStatus,
	image string,
) func() {
	return func() {
		p2, err := clientset.CoreV1().
			Pods(ns).
			Get(context.Background(), podName, metav1.GetOptions{})
		if err != nil {
			log.Printf("[SlowPull] 获取 Pod %s/%s 失败: %v", ns, podName, err)
			slowPullTimers.Delete(slowPullTimerKey)
			return
		}

		for _, newCs := range p2.Status.ContainerStatuses {
			if newCs.Name != cs.Name {
				continue
			}

			// 检查容器是否仍在等待且不是失败状态
			if newCs.ContainerID == "" &&
				newCs.State.Waiting != nil &&
				!isImagePullFailureReason(newCs.State.Waiting.Reason) &&
				!isBackOffPullingImage(
					newCs.State.Waiting.Reason,
					newCs.State.Waiting.Message,
				) {
				registry := parseRegistry(image)
				nodeName := getNodeName(p2)

				// 记录慢拉取状态
				slowPullInfo := slowPullInfo{
					namespace: ns,
					podName:   podName,
					nodeName:  nodeName,
					registry:  registry,
					image:     image,
				}
				slowPullTracking.Store(slowPullTimerKey, slowPullInfo)

				// 增加慢拉取指标
				imagePullSlowAlertGauge.WithLabelValues(ns, podName, nodeName, registry, image).
					Inc()
				imagePullSlowAlertCounter.WithLabelValues(ns, podName, nodeName, registry, image).
					Inc()

				log.Printf(
					"[SlowPullAlert] %s/%s container=%s node=%s registry=%s image=%s",
					ns,
					podName,
					cs.Name,
					nodeName,
					registry,
					image,
				)
			}

			break
		}

		slowPullTimers.Delete(slowPullTimerKey)
	}
}

func checkSlowPull(ns, podName string, cs corev1.ContainerStatus, image string) {
	slowPullTimerKey := fmt.Sprintf("%s/%s/%s", ns, podName, cs.Name)

	// 检查是否为 back-off pulling image 状态
	if cs.State.Waiting != nil &&
		isBackOffPullingImage(cs.State.Waiting.Reason, cs.State.Waiting.Message) {
		log.Printf(
			"[SlowPull] Detected back-off pulling image for %s/%s container=%s, cleaning up slow pull tracking",
			ns,
			podName,
			cs.Name,
		)
		cleanupSlowPull(slowPullTimerKey)

		return
	}

	// 检查容器是否已经启动成功
	if cs.ContainerID != "" {
		log.Printf(
			"[SlowPull] Container %s/%s container=%s started successfully, cleaning up slow pull tracking",
			ns,
			podName,
			cs.Name,
		)
		cleanupSlowPull(slowPullTimerKey)

		return
	}

	// 容器不在等待状态或者是失败状态，清理慢拉取状态
	if cs.State.Waiting == nil ||
		isImagePullFailureReason(cs.State.Waiting.Reason) ||
		!isPublicRegistry(image) {
		cleanupSlowPull(slowPullTimerKey)
		return
	}

	// 检查是否已经有定时器在运行
	if _, exists := slowPullTimers.Load(slowPullTimerKey); exists {
		// 定时器已存在，不需要重复创建
		return
	}

	timer := time.AfterFunc(
		5*time.Minute,
		newCheckSlowPullHandler(slowPullTimerKey, ns, podName, cs, image),
	)

	_, loaded := slowPullTimers.LoadOrStore(slowPullTimerKey, timer)
	if loaded {
		timer.Stop()
		log.Printf(
			"[SlowPull] Timer already exists for %s/%s container=%s, stopped duplicate timer",
			ns,
			podName,
			cs.Name,
		)
	}
}

func onPodAddOrUpdate(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}

	podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
	currentNodeName := getNodeName(pod)

	log.Printf(
		"[PodEvent] %s phase=%s uid=%s node=%s containers=%d",
		podKey,
		pod.Status.Phase,
		string(pod.UID),
		currentNodeName,
		len(pod.Status.ContainerStatuses),
	)

	// 遍历 InitContainerStatuses + ContainerStatuses
	for _, cs := range pod.Status.InitContainerStatuses {
		checkSlowPull(pod.Namespace, pod.Name, cs, cs.Image)
	}

	for _, cs := range pod.Status.ContainerStatuses {
		checkSlowPull(pod.Namespace, pod.Name, cs, cs.Image)
	}

	reasons := analyzePodImagePullErrors(pod, currentNodeName)

	piVal, _ := podFailures.LoadOrStore(podKey, &podInfo{
		reasons:   make(map[string]failureInfo),
		podName:   pod.Name,
		namespace: pod.Namespace,
	})

	pi, ok := piVal.(*podInfo)
	if !ok {
		log.Printf("[onPodAddOrUpdate] 无法解析已添加对象类型: %T", piVal)
		return
	}

	pi.mu.Lock()
	defer pi.mu.Unlock()

	pi.namespace = pod.Namespace
	pi.podName = pod.Name

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
		pi, ok := reasonsVal.(*podInfo)
		if !ok {
			log.Printf("[onPodDelete] 无法解析已删除对象类型: %T", reasonsVal)
			return
		}

		pi.mu.Lock()
		defer pi.mu.Unlock()

		// 使用存储的节点信息进行 Dec 操作，确保在正确的节点上执行
		for containerName, info := range pi.reasons {
			log.Printf(
				"[PodDelete] Dec gauge: namespace=%s pod=%s container=%s node=%s registry=%s image=%s reason=%s",
				pi.namespace,
				pi.podName,
				containerName,
				info.nodeName,
				info.registry,
				info.image,
				info.reason,
			)
			imagePullFailureGauge.WithLabelValues(pi.namespace, pi.podName, info.nodeName, info.registry, info.image, info.reason).
				Dec()
		}
	}

	// 清理慢拉取相关的状态
	prefix := key + "/"
	slowPullTimers.Range(func(k, v any) bool {
		sk, ok := k.(string)
		if !ok {
			log.Printf("[onPodDelete] 无法解析已删除对象类型: %T", k)
			return true
		}

		if strings.HasPrefix(sk, prefix) {
			if t, ok := v.(*time.Timer); ok {
				t.Stop()
			}

			slowPullTimers.Delete(sk)

			// 清理慢拉取状态
			cleanupSlowPull(sk)
		}

		return true
	})
}

// cleanupSlowPull 清理慢拉取状态
func cleanupSlowPull(slowPullTimerKey string) {
	if val, exists := slowPullTracking.LoadAndDelete(slowPullTimerKey); exists {
		if info, ok := val.(slowPullInfo); ok {
			imagePullSlowAlertGauge.WithLabelValues(info.namespace, info.podName, info.nodeName, info.registry, info.image).
				Dec()
			log.Printf(
				"[SlowPullCleanup] Dec slow pull gauge: namespace=%s pod=%s node=%s registry=%s image=%s",
				info.namespace,
				info.podName,
				info.nodeName,
				info.registry,
				info.image,
			)
		}
	}

	// 同时清理定时器
	if val, exists := slowPullTimers.LoadAndDelete(slowPullTimerKey); exists {
		if t, ok := val.(*time.Timer); ok {
			t.Stop()
			log.Printf("[SlowPullCleanup] Stopped timer for key: %s", slowPullTimerKey)
		}
	}
}

func analyzePodImagePullErrors(pod *corev1.Pod, nodeName string) map[string]failureInfo {
	reasons := make(map[string]failureInfo)

	checkContainerStatuses := func(statuses []corev1.ContainerStatus) {
		for _, cs := range statuses {
			if cs.State.Waiting == nil ||
				isImagePullFailureReason(cs.State.Waiting.Reason) ||
				!isPublicRegistry(cs.Image) {
				continue
			}

			classified := classifyFailureReason(
				cs.State.Waiting.Reason,
				cs.State.Waiting.Message,
			)
			registry := parseRegistry(cs.Image)
			// 使用容器名作为 key
			reasons[cs.Name] = failureInfo{
				registry: registry,
				nodeName: nodeName,   // 记录当前节点
				image:    cs.Image,   // 记录镜像信息
				reason:   classified, // 记录失败原因
			}

			// 如果是失败状态，清理对应的慢拉取状态
			key := fmt.Sprintf("%s/%s/%s", pod.Namespace, pod.Name, cs.Name)
			cleanupSlowPull(key)
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
	if !strings.Contains(image, "/") {
		return true
	}

	return strings.HasPrefix(image, "docker.io/") ||
		strings.HasPrefix(image, "gcr.io/") ||
		strings.HasPrefix(image, "ghcr.io/") ||
		strings.HasPrefix(image, "k8s.gcr.io/") ||
		strings.HasPrefix(image, "quay.io/") ||
		strings.HasPrefix(image, "registry.k8s.io/") ||
		(strings.HasPrefix(image, "registry.") && strings.Contains(image, ".aliyuncs.com/")) ||
		(strings.HasPrefix(image, "hub.") && strings.Contains(image, ".sealos.run/")) ||
		strings.HasPrefix(image, "sealos.hub")
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

func classifyFailureReason(r, message string) reason {
	lowMsg := strings.ToLower(message)
	switch strings.ToLower(r) {
	case "errimagepull", "imagepullbackoff":
		if reImageNotFound.MatchString(lowMsg) {
			return ReasonImageNotFound
		}

		if reProxyError.MatchString(lowMsg) {
			return ReasonProxyError
		}

		if reUnauthorized.MatchString(lowMsg) {
			return ReasonUnauthorized
		}

		if reTLS.MatchString(lowMsg) {
			return ReasonTLSHandshake
		}

		if strings.HasPrefix(lowMsg, "back-off pulling image") {
			return ReasonBackOff
		}

		log.Printf("[Classify] 未知错误分类 reason=%s message=%s", r, message)

		return ReasonUnknown
	default:
		return strings.ToLower(r)
	}
}

// isSpecificReason 判断是否为具体的失败原因（非 back_off_pulling_image）
func isSpecificReason(reason string) bool {
	return reason != ReasonBackOff && reason != ReasonUnknown
}

func updateReasons(
	pi *podInfo,
	reasons map[string]failureInfo,
	pod *corev1.Pod,
) {
	// 删除旧的原因 - 使用存储的节点信息
	for containerName, oldInfo := range pi.reasons {
		if _, found := reasons[containerName]; !found {
			log.Printf(
				"[UpdateReasons] Dec gauge: namespace=%s pod=%s container=%s node=%s registry=%s image=%s reason=%s",
				pi.namespace,
				pi.podName,
				containerName,
				oldInfo.nodeName,
				oldInfo.registry,
				oldInfo.image,
				oldInfo.reason,
			)
			imagePullFailureGauge.WithLabelValues(pi.namespace, pi.podName, oldInfo.nodeName, oldInfo.registry, oldInfo.image, oldInfo.reason).
				Dec()
			delete(pi.reasons, containerName)
		}
	}

	// 添加新的原因
	for containerName, info := range reasons {
		oldInfo, found := pi.reasons[containerName]
		if found {
			// 检查是否需要保留原有的具体原因
			finalReason := info.reason

			// 如果新的原因是 back_off_pulling_image，且其他信息没变，且之前有具体原因，则保留具体原因
			if info.reason == ReasonBackOff &&
				oldInfo.nodeName == info.nodeName &&
				oldInfo.image == info.image &&
				oldInfo.registry == info.registry &&
				isSpecificReason(oldInfo.reason) {
				finalReason = oldInfo.reason
				log.Printf(
					"[UpdateReasons] Preserving specific reason for %s/%s container=%s: keeping '%s' instead of 'back_off_pulling_image'",
					pi.namespace,
					pi.podName,
					containerName,
					oldInfo.reason,
				)
			}

			// 检查是否有变化（节点、失败原因、镜像等）
			if oldInfo.nodeName != info.nodeName || oldInfo.reason != finalReason ||
				oldInfo.image != info.image {
				log.Printf(
					"[UpdateReasons] Info changed for %s/%s container=%s: node=%s->%s, reason=%s->%s, image=%s->%s",
					pi.namespace,
					pi.podName,
					containerName,
					oldInfo.nodeName,
					info.nodeName,
					oldInfo.reason,
					finalReason,
					oldInfo.image,
					info.image,
				)

				// 在旧信息上 Dec
				imagePullFailureGauge.WithLabelValues(pi.namespace, pi.podName, oldInfo.nodeName, oldInfo.registry, oldInfo.image, oldInfo.reason).
					Dec()
				// 在新信息上 Inc
				imagePullFailureGauge.WithLabelValues(pi.namespace, pi.podName, info.nodeName, info.registry, info.image, finalReason).
					Inc()
				imagePullFailureAlertCounter.WithLabelValues(pi.namespace, pi.podName, info.nodeName, info.registry, info.image, finalReason).
					Inc()

				// 更新存储的信息，使用最终确定的原因
				info.reason = finalReason
				pi.reasons[containerName] = info
			} else if oldInfo.reason != finalReason {
				// 只有原因发生变化的情况
				log.Printf(
					"[UpdateReasons] Only reason changed for %s/%s container=%s: %s->%s",
					pi.namespace,
					pi.podName,
					containerName,
					oldInfo.reason,
					finalReason,
				)

				imagePullFailureGauge.WithLabelValues(pi.namespace, pi.podName, oldInfo.nodeName, oldInfo.registry, oldInfo.image, oldInfo.reason).
					Dec()
				imagePullFailureGauge.WithLabelValues(pi.namespace, pi.podName, info.nodeName, info.registry, info.image, finalReason).
					Inc()
				imagePullFailureAlertCounter.WithLabelValues(pi.namespace, pi.podName, info.nodeName, info.registry, info.image, finalReason).
					Inc()

				info.reason = finalReason
				pi.reasons[containerName] = info
			}

			continue
		}

		// 全新的失败容器
		log.Printf(
			"[UpdateReasons] Inc gauge: namespace=%s pod=%s container=%s node=%s registry=%s image=%s reason=%s",
			pi.namespace,
			pi.podName,
			containerName,
			info.nodeName,
			info.registry,
			info.image,
			info.reason,
		)
		imagePullFailureGauge.WithLabelValues(pi.namespace, pi.podName, info.nodeName, info.registry, info.image, info.reason).
			Inc()
		imagePullFailureAlertCounter.WithLabelValues(pi.namespace, pi.podName, info.nodeName, info.registry, info.image, info.reason).
			Inc()

		countKey := fmt.Sprintf(
			"%s/%s/%s/%s/%s/%s",
			pi.namespace,
			pi.podName,
			info.nodeName,
			info.registry,
			info.image,
			info.reason,
		)
		acVal, _ := alertCounts.LoadOrStore(countKey, &alertCount{})

		ac, ok := acVal.(*alertCount)
		if !ok {
			log.Printf("[updateReasons] 无法解析 alertCount 类型: %T", acVal)
			continue
		}

		newCount := ac.count.Add(1)
		log.Printf(
			"[AlertCounter] #%d %s exported_namespace=%s exported_pod=%s container=%s node=%s registry=%s image=%s reason=%s",
			newCount,
			pod.Name,
			pi.namespace,
			pi.podName,
			containerName,
			info.nodeName,
			info.registry,
			info.image,
			info.reason,
		)

		pi.reasons[containerName] = info
	}
}

func main() {
	// 注册 Prometheus 指标
	prometheus.MustRegister(imagePullFailureGauge)
	prometheus.MustRegister(imagePullFailureAlertCounter)
	prometheus.MustRegister(imagePullSlowAlertGauge)
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
		Addr:              ":8080",
		Handler:           promhttp.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
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
