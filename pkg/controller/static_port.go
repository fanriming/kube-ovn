package controller

import (
	"fmt"
	"reflect"

	kubeovnv1 "github.com/kubeovn/kube-ovn/pkg/apis/kubeovn/v1"
	"github.com/kubeovn/kube-ovn/pkg/util"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

func (c *Controller) enqueueAddStaticPort(obj interface{}) {
	if !c.isLeader() {
		return
	}
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	klog.V(3).Infof("enqueue add securityGroup %s", key)
	c.addOrUpdateStaticPortQueue.Add(key)
}

func (c *Controller) enqueueUpdateStaticPort(old, new interface{}) {
	if !c.isLeader() {
		return
	}
	oldSg := old.(*kubeovnv1.StaticPort)
	newSg := new.(*kubeovnv1.StaticPort)
	if !reflect.DeepEqual(oldSg.Spec, newSg.Spec) {
		var key string
		var err error
		if key, err = cache.MetaNamespaceKeyFunc(new); err != nil {
			utilruntime.HandleError(err)
			return
		}
		klog.V(3).Infof("enqueue update securityGroup %s", key)
		c.addOrUpdateStaticPortQueue.Add(key)
	}
}

func (c *Controller) enqueueDeletStaticPort(obj interface{}) {
	if !c.isLeader() {
		return
	}
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	klog.V(3).Infof("enqueue delete securityGroup %s", key)
	c.delStaticPortQueue.Add(key)
}

func (c *Controller) runAddStaticPortWorker() {
	for c.processNextAddOrUpdateStaticPortWorkItem() {
	}
}

func (c *Controller) runDelStaticPortWorker() {
	for c.processNextDeleteStaticPortWorkItem() {
	}
}

func (c *Controller) processNextAddOrUpdateStaticPortWorkItem() bool {
	obj, shutdown := c.addOrUpdateStaticPortQueue.Get()

	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.addOrUpdateStaticPortQueue.Done(obj)
		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			c.addOrUpdateStaticPortQueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		if err := c.handleAddOrUpdateSg(key); err != nil {
			c.addOrUpdateStaticPortQueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		c.addOrUpdateStaticPortQueue.Forget(obj)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}
	return true
}

func (c *Controller) processNextDeleteStaticPortWorkItem() bool {
	obj, shutdown := c.delStaticPortQueue.Get()

	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.delStaticPortQueue.Done(obj)
		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			c.delStaticPortQueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		if err := c.handleDeleteSg(key); err != nil {
			c.delStaticPortQueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		c.delStaticPortQueue.Forget(obj)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}
	return true
}

func (c *Controller) handleAddOrUpdateStaticPort(key string) error {
	port, err := c.staticPortLister.Get(key)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	subnet, err := c.subnetsLister.Get(port.Spec.Subnet)
	if err != nil {
		return err
	}

	ipStr := util.GetStringIP(port.Spec.V4IP, port.Spec.V6IP)
	tag, err := c.getSubnetVlanTag(subnet)
	if err != nil {
		return err
	}

	// create port
	if err := c.ovnClient.CreatePort(port.Spec.Subnet, port.Name, ipStr, subnet.Spec.CIDRBlock, port.Spec.Mac, tag, "", "", true, ""); err != nil {
		return err
	}

	// update ipam

	return nil
}

func (c *Controller) handleDeleteStaticPort(key string) error {
	return nil
}
