package controller

import (
	"fmt"
	"strings"

	kubeovnv1 "github.com/alauda/kube-ovn/pkg/apis/kubeovn/v1"
	"github.com/alauda/kube-ovn/pkg/util"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

func (c *Controller) enqueueDeleteService(obj interface{}) {
	if !c.isLeader() {
		return
	}
	svc := obj.(*v1.Service)
	klog.V(3).Infof("enqueue delete service %s/%s", svc.Namespace, svc.Name)
	if svc.Spec.ClusterIP != v1.ClusterIPNone && svc.Spec.ClusterIP != "" {
		for _, port := range svc.Spec.Ports {
			if port.Protocol == v1.ProtocolTCP {
				c.deleteTcpServiceQueue.Add(fmt.Sprintf("%s:%d", svc.Spec.ClusterIP, port.Port))
			} else if port.Protocol == v1.ProtocolUDP {
				c.deleteUdpServiceQueue.Add(fmt.Sprintf("%s:%d", svc.Spec.ClusterIP, port.Port))
			}
		}
	}
}

func (c *Controller) enqueueUpdateService(old, new interface{}) {
	if !c.isLeader() {
		return
	}
	oldSvc := old.(*v1.Service)
	newSvc := new.(*v1.Service)
	if oldSvc.ResourceVersion == newSvc.ResourceVersion {
		return
	}

	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(new); err != nil {
		utilruntime.HandleError(err)
		return
	}
	klog.V(3).Infof("enqueue update service %s", key)
	c.updateServiceQueue.Add(key)
}

func (c *Controller) runDeleteTcpServiceWorker() {
	for c.processNextDeleteTcpServiceWorkItem() {
	}
}

func (c *Controller) runDeleteUdpServiceWorker() {
	for c.processNextDeleteUdpServiceWorkItem() {
	}
}

func (c *Controller) runUpdateServiceWorker() {
	for c.processNextUpdateServiceWorkItem() {
	}
}

func (c *Controller) processNextDeleteTcpServiceWorkItem() bool {
	obj, shutdown := c.deleteTcpServiceQueue.Get()

	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.deleteTcpServiceQueue.Done(obj)
		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			c.deleteTcpServiceQueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		if err := c.handleDeleteService(key, v1.ProtocolTCP); err != nil {
			c.deleteTcpServiceQueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		c.deleteTcpServiceQueue.Forget(obj)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}
	return true
}

func (c *Controller) processNextDeleteUdpServiceWorkItem() bool {
	obj, shutdown := c.deleteUdpServiceQueue.Get()

	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.deleteUdpServiceQueue.Done(obj)
		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			c.deleteUdpServiceQueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		if err := c.handleDeleteService(key, v1.ProtocolUDP); err != nil {
			c.deleteUdpServiceQueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		c.deleteUdpServiceQueue.Forget(obj)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}
	return true
}

func (c *Controller) processNextUpdateServiceWorkItem() bool {
	obj, shutdown := c.updateServiceQueue.Get()

	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.updateServiceQueue.Done(obj)
		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			c.updateServiceQueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		if err := c.handleUpdateService(key); err != nil {
			c.updateServiceQueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		c.updateServiceQueue.Forget(obj)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}
	return true
}

func (c *Controller) handleDeleteService(vip string, protocol v1.Protocol) error {
	svcs, err := c.servicesLister.Services(v1.NamespaceAll).List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to list svc, %v", err)
		return err
	}
	for _, svc := range svcs {
		if svc.Spec.ClusterIP == parseVipAddr(vip) {
			return nil
		}
	}

	if protocol == v1.ProtocolTCP {
		if err := c.ovnClient.DeleteLoadBalancerVip(vip, c.config.ClusterTcpLoadBalancer); err != nil {
			klog.Errorf("failed to delete vip %s from tcp lb, %v", vip, err)
			return err
		}
		if err := c.ovnClient.DeleteLoadBalancerVip(vip, c.config.ClusterTcpSessionLoadBalancer); err != nil {
			klog.Errorf("failed to delete vip %s from tcp session lb, %v", vip, err)
			return err
		}
	} else {
		if err := c.ovnClient.DeleteLoadBalancerVip(vip, c.config.ClusterUdpSessionLoadBalancer); err != nil {
			klog.Errorf("failed to delete vip %s from udp session lb, %v", vip, err)
			return err
		}
	}

	return nil
}

func (c *Controller) handleUpdateService(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}
	klog.Infof("update svc %s/%s", namespace, name)
	svc, err := c.servicesLister.Services(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	ip := svc.Spec.ClusterIP
	if ip == "" || ip == v1.ClusterIPNone {
		return nil
	}

	tcpVips := []string{}
	udpVips := []string{}
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
	oTcpLb, oUdpLb := vpc.Status.TcpSessionLoadBalancer, vpc.Status.UdpSessionLoadBalancer
	if svc.Spec.SessionAffinity == v1.ServiceAffinityClientIP {
		tcpLb, udpLb = vpc.Status.TcpSessionLoadBalancer, vpc.Status.UdpSessionLoadBalancer
		oTcpLb, oUdpLb = vpc.Status.TcpLoadBalancer, vpc.Status.UdpLoadBalancer
	}

	for _, port := range svc.Spec.Ports {
		if port.Protocol == v1.ProtocolTCP {
			if util.CheckProtocol(ip) == kubeovnv1.ProtocolIPv6 {
				tcpVips = append(tcpVips, fmt.Sprintf("[%s]:%d", ip, port.Port))
			} else {
				tcpVips = append(tcpVips, fmt.Sprintf("%s:%d", ip, port.Port))
			}
		} else if port.Protocol == v1.ProtocolUDP {
			if util.CheckProtocol(ip) == kubeovnv1.ProtocolIPv6 {
				udpVips = append(udpVips, fmt.Sprintf("[%s]:%d", ip, port.Port))
			} else {
				udpVips = append(udpVips, fmt.Sprintf("%s:%d", ip, port.Port))
			}
		}
	}
	// for service update
	lbUuid, err := c.ovnClient.FindLoadbalancer(tcpLb)
	if err != nil {
		klog.Errorf("failed to get lb %v", err)
		return err
	}
	vips, err := c.ovnClient.GetLoadBalancerVips(lbUuid)
	if err != nil {
		klog.Errorf("failed to get tcp lb vips %v", err)
		return err
	}
	klog.V(3).Infof("exist tcp vips are %v", vips)
	for _, vip := range tcpVips {
		if err := c.ovnClient.DeleteLoadBalancerVip(vip, oTcpLb); err != nil {
			klog.Errorf("failed to delete lb %s form %s, %v", vip, oTcpLb, err)
			return err
		}
		if _, ok := vips[vip]; !ok {
			klog.Infof("add vip %s to tcp lb %s", vip, oTcpLb)
			c.updateEndpointQueue.Add(key)
			break
		}
	}

	for vip := range vips {
		if parseVipAddr(vip) == ip && !util.IsStringIn(vip, tcpVips) {
			klog.Infof("remove stall vip %s", vip)
			err := c.ovnClient.DeleteLoadBalancerVip(vip, tcpLb)
			if err != nil {
				klog.Errorf("failed to delete vip %s from tcp lb %v", vip, err)
				return err
			}
		}
	}

	lbUuid, err = c.ovnClient.FindLoadbalancer(udpLb)
	if err != nil {
		klog.Errorf("failed to get lb %v", err)
		return err
	}
	vips, err = c.ovnClient.GetLoadBalancerVips(lbUuid)
	if err != nil {
		klog.Errorf("failed to get udp lb vips %v", err)
		return err
	}
	klog.Infof("exist udp vips are %v", vips)
	for _, vip := range udpVips {
		if err := c.ovnClient.DeleteLoadBalancerVip(vip, oUdpLb); err != nil {
			klog.Errorf("failed to delete lb %s form %s, %v", vip, oUdpLb, err)
			return err
		}
		if _, ok := vips[vip]; !ok {
			klog.Infof("add vip %s to udp lb %s", vip, oUdpLb)
			c.updateEndpointQueue.Add(key)
			break
		}
	}

	for vip := range vips {
		if parseVipAddr(vip) == ip && !util.IsStringIn(vip, udpVips) {
			klog.Infof("remove stall vip %s", vip)
			err := c.ovnClient.DeleteLoadBalancerVip(vip, udpLb)
			if err != nil {
				klog.Errorf("failed to delete vip %s from udp lb %v", vip, err)
				return err
			}
		}
	}

	return nil
}

// The type of vips is map, which format is like [fd00:10:96::11c9]:10665:[fc00:f853:ccd:e793::2]:10665,[fc00:f853:ccd:e793::3]:10665
// Parse key of map, [fd00:10:96::11c9]:10665 for example
func parseVipAddr(vipStr string) string {
	var vip string
	if strings.ContainsAny(vipStr, "[]") {
		vip = strings.Trim(strings.Split(vipStr, "]")[0], "[]")
	} else {
		vip = strings.Split(vipStr, ":")[0]
	}
	return vip
}
