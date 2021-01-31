package controller

import (
	"fmt"
	"strings"

	kubeovnv1 "github.com/alauda/kube-ovn/pkg/apis/kubeovn/v1"

	"github.com/alauda/kube-ovn/pkg/util"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

func (c *Controller) enqueueAddEndpoint(obj interface{}) {
	if !c.isLeader() {
		return
	}
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	klog.V(3).Infof("enqueue add endpoint %s", key)
	c.updateEndpointQueue.Add(key)
}

func (c *Controller) enqueueUpdateEndpoint(old, new interface{}) {
	if !c.isLeader() {
		return
	}
	oldEp := old.(*v1.Endpoints)
	newEp := new.(*v1.Endpoints)
	if oldEp.ResourceVersion == newEp.ResourceVersion {
		return
	}

	if len(oldEp.Subsets) == 0 && len(newEp.Subsets) == 0 {
		return
	}

	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(new); err != nil {
		utilruntime.HandleError(err)
		return
	}
	klog.V(3).Infof("enqueue update endpoint %s", key)
	c.updateEndpointQueue.Add(key)
}

func (c *Controller) runUpdateEndpointWorker() {
	for c.processNextUpdateEndpointWorkItem() {
	}
}

func (c *Controller) processNextUpdateEndpointWorkItem() bool {
	obj, shutdown := c.updateEndpointQueue.Get()

	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.updateEndpointQueue.Done(obj)
		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			c.updateEndpointQueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		if err := c.handleUpdateEndpoint(key); err != nil {
			c.updateEndpointQueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		c.updateEndpointQueue.Forget(obj)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}
	return true
}

func (c *Controller) handleUpdateEndpoint(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}
	klog.Infof("update endpoint %s/%s", namespace, name)

	ep, err := c.endpointsLister.Endpoints(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	svc, err := c.servicesLister.Services(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	clusterIP := svc.Spec.ClusterIP
	if clusterIP == "" || clusterIP == v1.ClusterIPNone {
		return nil
	}

	vpcName := svc.Annotations[util.VpcNameLabel]
	if vpcName == "" {
		vpcName = util.DefaultVpc
	}
	vpc, err := c.vpcsLister.Get(vpcName)
	if err != nil {
		klog.Errorf("failed to get vpc %s of lb, %v", vpcName, err)
		return err
	}

	tcpLb, udpLb := vpc.Status.TcpLoadBalancer, vpc.Status.UdpLoadBalancer
	if svc.Spec.SessionAffinity == v1.ServiceAffinityClientIP {
		tcpLb, udpLb = vpc.Status.TcpSessionLoadBalancer, vpc.Status.UdpSessionLoadBalancer
	}

	for _, port := range svc.Spec.Ports {
		var vip string
		if util.CheckProtocol(clusterIP) == kubeovnv1.ProtocolIPv6 {
			vip = fmt.Sprintf("[%s]:%d", clusterIP, port.Port)
		} else {
			vip = fmt.Sprintf("%s:%d", clusterIP, port.Port)
		}

		backends := getServicePortBackends(ep, port, clusterIP)
		if port.Protocol == v1.ProtocolTCP {
			// for performance reason delete lb with no backends
			if len(backends) > 0 {
				err = c.ovnClient.CreateLoadBalancerRule(tcpLb, vip, getServicePortBackends(ep, port, clusterIP), string(port.Protocol))
				if err != nil {
					klog.Errorf("failed to update vip %s to tcp lb, %v", vip, err)
					return err
				}
			} else {
				err = c.ovnClient.DeleteLoadBalancerVip(vip, tcpLb)
				if err != nil {
					klog.Errorf("failed to delete vip %s at tcp lb, %v", vip, err)
					return err
				}
			}
		} else {
			if len(backends) > 0 {
				err = c.ovnClient.CreateLoadBalancerRule(udpLb, vip, getServicePortBackends(ep, port, clusterIP), string(port.Protocol))
				if err != nil {
					klog.Errorf("failed to update vip %s to udp lb, %v", vip, err)
					return err
				}
			} else {
				err = c.ovnClient.DeleteLoadBalancerVip(vip, udpLb)
				if err != nil {
					klog.Errorf("failed to delete vip %s at udp lb, %v", vip, err)
					return err
				}
			}
		}
	}
	return nil
}

func getServicePortBackends(endpoints *v1.Endpoints, servicePort v1.ServicePort, serviceIP string) string {
	backends := []string{}
	for _, subset := range endpoints.Subsets {
		var targetPort int32
		for _, port := range subset.Ports {
			if port.Name == servicePort.Name {
				targetPort = port.Port
				break
			}
		}
		if targetPort == 0 {
			continue
		}

		for _, address := range subset.Addresses {
			if util.CheckProtocol(serviceIP) == util.CheckProtocol(address.IP) {
				if util.CheckProtocol(address.IP) == kubeovnv1.ProtocolIPv6 {
					backends = append(backends, fmt.Sprintf("[%s]:%d", address.IP, targetPort))
				} else {
					backends = append(backends, fmt.Sprintf("%s:%d", address.IP, targetPort))
				}
			}
		}
	}
	return strings.Join(backends, ",")
}
