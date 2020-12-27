package controller

import (
	"fmt"
	"strings"
	"time"

	"github.com/alauda/kube-ovn/pkg/ovs"
	"github.com/alauda/kube-ovn/pkg/util"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog"
)

var lastNoPodLSP map[string]bool

func (c *Controller) gc() error {
	gcFunctions := []func() error{
		c.gcNode,
		c.gcLogicalSwitch,
		c.gcCustomLogicalRouter,
		c.gcLogicalSwitchPort,
		c.gcLoadBalancer,
		c.gcPortGroup,
		c.gcStaticRoute,
		c.gcPortChainClassifier,
		c.gcPortPairGroup,
		c.gcPortChain,
		c.gcPortPair,
	}
	for _, gcFunc := range gcFunctions {
		if err := gcFunc(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) gcLogicalSwitch() error {
	klog.Infof("start to gc logical switch")
	subnets, err := c.subnetsLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to list subnet, %v", err)
		return err
	}
	subnetNames := make([]string, 0, len(subnets))
	for _, s := range subnets {
		subnetNames = append(subnetNames, s.Name)
	}
	lss, err := c.ovnClient.ListLogicalSwitch()
	if err != nil {
		klog.Errorf("failed to list logical switch, %v", err)
		return err
	}
	klog.Infof("ls in ovn %v", lss)
	klog.Infof("subnet in kubernetes %v", subnetNames)
	for _, ls := range lss {
		if ls == util.InterconnectionSwitch || ls == util.ExternalGatewaySwitch {
			continue
		}
		if !util.IsStringIn(ls, subnetNames) {
			klog.Infof("gc subnet %s", ls)
			if err := c.handleDeleteSubnet(ls); err != nil {
				klog.Errorf("failed to gc subnet %s, %v", ls, err)
				return err
			}
		}
	}
	return nil
}

func (c *Controller) gcCustomLogicalRouter() error {
	klog.Infof("start to gc logical router")
	vpcs, err := c.vpcsLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to list vpc, %v", err)
		return err
	}
	vpcNames := make([]string, 0, len(vpcs))
	for _, s := range vpcs {
		vpcNames = append(vpcNames, s.Name)
	}
	lrs, err := c.ovnClient.ListLogicalRouter()
	if err != nil {
		klog.Errorf("failed to list logical router, %v", err)
		return err
	}
	klog.Infof("lr in ovn %v", lrs)
	klog.Infof("vpc in kubernetes %v", vpcNames)
	for _, lr := range lrs {
		if lr == util.DefaultVpc {
			continue
		}
		if !util.IsStringIn(lr, vpcNames) {
			klog.Infof("gc router %s", lr)
			if err := c.deleteVpcRouter(lr); err != nil {
				klog.Errorf("failed to delete router %s, %v", lr, err)
				return err
			}
		}
	}
	return nil
}

func (c *Controller) gcNode() error {
	klog.Infof("start to gc nodes")
	nodes, err := c.nodesLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to list node, %v", err)
		return err
	}
	nodeNames := make([]string, 0, len(nodes))
	for _, no := range nodes {
		nodeNames = append(nodeNames, no.Name)
	}
	ips, err := c.ipsLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to list ip, %v", err)
		return err
	}
	ipNodeNames := make([]string, 0, len(ips))
	for _, ip := range ips {
		if !strings.Contains(ip.Name, ".") {
			ipNodeNames = append(ipNodeNames, strings.TrimPrefix(ip.Name, "node-"))
		}
	}
	for _, no := range ipNodeNames {
		if !util.IsStringIn(no, nodeNames) {
			klog.Infof("gc node %s", no)
			if err := c.handleDeleteNode(no); err != nil {
				klog.Errorf("failed to gc node %s, %v", no, err)
				return err
			}
		}
	}
	return nil
}

func (c *Controller) gcLogicalSwitchPort() error {
	klog.Info("start to gc logical switch port")
	if err := c.markAndCleanLSP(); err != nil {
		return err
	}
	time.Sleep(3 * time.Second)
	return c.markAndCleanLSP()
}

func (c *Controller) markAndCleanLSP() error {
	klog.V(4).Infof("start to gc logical switch ports")
	pods, err := c.podsLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to list ip, %v", err)
		return err
	}
	nodes, err := c.nodesLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to list node, %v", err)
		return err
	}
	ipNames := make([]string, 0, len(pods)+len(nodes))
	for _, pod := range pods {
		if isPodAlive(pod) && pod.Annotations[util.AllocatedAnnotation] == "true" {
			ipNames = append(ipNames, fmt.Sprintf("%s.%s", pod.Name, pod.Namespace))
		}
	}
	for _, node := range nodes {
		ipNames = append(ipNames, fmt.Sprintf("node-%s", node.Name))
	}
	lsps, err := c.ovnClient.ListLogicalSwitchPort()
	if err != nil {
		klog.Errorf("failed to list logical switch port, %v", err)
		return err
	}

	noPodLSP := map[string]bool{}
	for _, lsp := range lsps {
		if !util.IsStringIn(lsp, ipNames) {
			if lastNoPodLSP[lsp] == false {
				noPodLSP[lsp] = true
			} else {
				klog.Infof("gc logical switch port %s", lsp)
				if err := c.ovnClient.DeleteLogicalSwitchPort(lsp); err != nil {
					klog.Errorf("failed to delete lsp %s, %v", lsp, err)
					return err
				}
				if err := c.config.KubeOvnClient.KubeovnV1().IPs().Delete(lsp, &metav1.DeleteOptions{}); err != nil {
					if !k8serrors.IsNotFound(err) {
						klog.Errorf("failed to delete ip %s, %v", lsp, err)
						return err
					}
				}
			}
		}
	}

	lastNoPodLSP = noPodLSP
	return nil
}

func (c *Controller) gcLoadBalancer() error {
	klog.Infof("start to gc loadbalancers")
	svcs, err := c.servicesLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to list svc, %v", err)
		return err
	}
	tcpVips := []string{}
	udpVips := []string{}
	tcpSessionVips := []string{}
	udpSessionVips := []string{}
	for _, svc := range svcs {
		ip := svc.Spec.ClusterIP
		for _, port := range svc.Spec.Ports {
			if port.Protocol == corev1.ProtocolTCP {
				if svc.Spec.SessionAffinity == corev1.ServiceAffinityClientIP {
					tcpSessionVips = append(tcpSessionVips, fmt.Sprintf("%s:%d", ip, port.Port))
				} else {
					tcpVips = append(tcpVips, fmt.Sprintf("%s:%d", ip, port.Port))
				}
			} else {
				if svc.Spec.SessionAffinity == corev1.ServiceAffinityClientIP {
					udpSessionVips = append(udpSessionVips, fmt.Sprintf("%s:%d", ip, port.Port))
				} else {
					udpVips = append(udpVips, fmt.Sprintf("%s:%d", ip, port.Port))
				}
			}
		}
	}

	lbUuid, err := c.ovnClient.FindLoadbalancer(c.config.ClusterTcpLoadBalancer)
	if err != nil {
		klog.Errorf("failed to get lb %v", err)
	}
	vips, err := c.ovnClient.GetLoadBalancerVips(lbUuid)
	if err != nil {
		klog.Errorf("failed to get tcp lb vips %v", err)
		return err
	}
	for vip := range vips {
		if !util.IsStringIn(vip, tcpVips) {
			err := c.ovnClient.DeleteLoadBalancerVip(vip, c.config.ClusterTcpLoadBalancer)
			if err != nil {
				klog.Errorf("failed to delete vip %s from tcp lb, %v", vip, err)
				return err
			}
		}
	}

	lbUuid, err = c.ovnClient.FindLoadbalancer(c.config.ClusterTcpSessionLoadBalancer)
	if err != nil {
		klog.Errorf("failed to get lb %v", err)
	}
	vips, err = c.ovnClient.GetLoadBalancerVips(lbUuid)
	if err != nil {
		klog.Errorf("failed to get tcp session lb vips %v", err)
		return err
	}
	for vip := range vips {
		if !util.IsStringIn(vip, tcpSessionVips) {
			err := c.ovnClient.DeleteLoadBalancerVip(vip, c.config.ClusterTcpSessionLoadBalancer)
			if err != nil {
				klog.Errorf("failed to delete vip %s from tcp session lb, %v", vip, err)
				return err
			}
		}
	}

	lbUuid, err = c.ovnClient.FindLoadbalancer(c.config.ClusterUdpLoadBalancer)
	if err != nil {
		klog.Errorf("failed to get lb %v", err)
		return err
	}
	vips, err = c.ovnClient.GetLoadBalancerVips(lbUuid)
	if err != nil {
		klog.Errorf("failed to get udp lb vips %v", err)
		return err
	}
	for vip := range vips {
		if !util.IsStringIn(vip, udpVips) {
			err := c.ovnClient.DeleteLoadBalancerVip(vip, c.config.ClusterUdpLoadBalancer)
			if err != nil {
				klog.Errorf("failed to delete vip %s from tcp lb, %v", vip, err)
				return err
			}
		}
	}

	lbUuid, err = c.ovnClient.FindLoadbalancer(c.config.ClusterUdpSessionLoadBalancer)
	if err != nil {
		klog.Errorf("failed to get lb %v", err)
		return err
	}
	vips, err = c.ovnClient.GetLoadBalancerVips(lbUuid)
	if err != nil {
		klog.Errorf("failed to get udp session lb vips %v", err)
		return err
	}
	for vip := range vips {
		if !util.IsStringIn(vip, udpVips) {
			err := c.ovnClient.DeleteLoadBalancerVip(vip, c.config.ClusterUdpSessionLoadBalancer)
			if err != nil {
				klog.Errorf("failed to delete vip %s from udp session lb, %v", vip, err)
				return err
			}
		}
	}

	return nil
}

func (c *Controller) gcPortGroup() error {
	klog.Infof("start to gc network policy")
	nps, err := c.npsLister.List(labels.Everything())
	npNames := make([]string, 0, len(nps))
	for _, np := range nps {
		npNames = append(npNames, fmt.Sprintf("%s/%s", np.Namespace, np.Name))
	}
	if err != nil {
		klog.Errorf("failed to list network policy, %v", err)
		return err
	}
	pgs, err := c.ovnClient.ListPortGroup()
	if err != nil {
		klog.Errorf("failed to list port-group, %v", err)
		return err
	}
	for _, pg := range pgs {
		if !util.IsStringIn(fmt.Sprintf("%s/%s", pg.NpNamespace, pg.NpName), npNames) {
			klog.Infof("gc port group %s", pg.Name)
			if err := c.handleDeleteNp(fmt.Sprintf("%s/%s", pg.NpNamespace, pg.NpName)); err != nil {
				klog.Errorf("failed to gc np %s/%s, %v", pg.NpNamespace, pg.NpName, err)
				return err
			}
		}
	}
	return nil
}

func (c *Controller) gcStaticRoute() error {
	klog.Infof("start to gc static routes")
	routes, err := c.ovnClient.ListStaticRoute()
	if err != nil {
		klog.Errorf("failed to list static route %v", err)
		return err
	}
	for _, route := range routes {
		if route.Policy == ovs.PolicyDstIP || route.Policy == "" {
			if !c.ipam.ContainAddress(route.NextHop) {
				klog.Infof("gc static route %s %s %s", route.Policy, route.CIDR, route.NextHop)
				if err := c.ovnClient.DeleteStaticRouteByNextHop(route.NextHop); err != nil {
					klog.Errorf("failed to delete stale nexthop route %s, %v", route.NextHop, err)
				}
			}
		} else {
			if strings.Contains(route.CIDR, "/") {
				continue
			}
			if !c.ipam.ContainAddress(route.CIDR) {
				klog.Infof("gc static route %s %s %s", route.Policy, route.CIDR, route.NextHop)
				if err := c.ovnClient.DeleteStaticRoute(route.CIDR, c.config.ClusterRouter); err != nil {
					klog.Errorf("failed to delete stale route %s, %v", route.NextHop, err)
				}
			}
		}
	}
	return nil
}

func (c *Controller) gcPortChainClassifier() error {
	klog.Infof("start to gc static port chain classifier")
	sfcs, err := c.sfcLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to list sfc, %v", err)
		return err
	}
	sfcNames := make([]string, 0, len(sfcs))
	for _, sfc := range sfcs {
		sfcNames = append(sfcNames, sfc.Name)
	}

	pccs, err := c.ovnClient.ListLogicalPortChainClassifier()
	if err != nil {
		klog.Errorf("failed to list port chain classifier %v", err)
		return err
	}
	for _, pcc := range pccs {
		result := strings.Split(pcc, "-pcc-")
		if len(result) == 2 && util.ContainsString(sfcNames, result[0]) {
			continue
		}
		if err := c.ovnClient.DelPortChainClassifier(pcc); err != nil {
			klog.Errorf("failed to delete classifier %s , %v", pcc, err)
			return err
		}
	}
	return nil
}

func (c *Controller) gcPortPairGroup() error {
	klog.Infof("start to gc port pair group")
	vgs, err := c.vnfGroupLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to list sfc, %v", err)
		return err
	}
	vgNames := make([]string, 0, len(vgs))
	for _, vg := range vgs {
		vgNames = append(vgNames, vg.Name)
	}

	ppgs, err := c.ovnClient.ListLogicalPortPairGroup("")
	if err != nil {
		klog.Errorf("failed to list port pair group %v", err)
		return err
	}
	for _, ppg := range ppgs {
		result := strings.Split(ppg, "-ppg-")
		if len(result) == 2 && util.ContainsString(vgNames, result[1]) {
			continue
		}
		if err := c.ovnClient.DelLogicalPortPairGroup(ppg); err != nil {
			klog.Errorf("failed to delete port pair group %s , %v", ppg, err)
			return err
		}

	}
	return nil
}

func (c *Controller) gcPortPair() error {
	klog.Infof("start to gc port pair")
	pps, err := c.ovnClient.ListLogicalPortPair()
	if err != nil {
		klog.Errorf("failed to list port pair, %v", err)
		return err
	}

	vgs, err := c.vnfGroupLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to list sfc, %v", err)
		return err
	}

	var specPortPairs []string
	for _, vg := range vgs {
		for _, port := range vg.Status.Ports {
			specPortPairs = append(specPortPairs, genPortPairName(vg.Name, port))
		}
	}

	for _, pp := range pps {
		if util.ContainsString(specPortPairs, pp) {
			continue
		}
		if err := c.ovnClient.DelLogicalPortPair(pp); err != nil {
			klog.Errorf("failed to delete port pair %s , %v", pp, err)
			return err
		}
	}
	return nil
}

func (c *Controller) gcPortChain() error {
	klog.Infof("start to gc static port chain")
	sfcs, err := c.sfcLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to list sfc, %v", err)
		return err
	}
	sfcNames := make([]string, 0, len(sfcs))
	for _, sfc := range sfcs {
		sfcNames = append(sfcNames, sfc.Name)
	}

	pcs, err := c.ovnClient.ListPortChain()
	if err != nil {
		klog.Errorf("failed to list port chain, %v", err)
		return err
	}

	for _, pc := range pcs {
		if util.ContainsString(sfcNames, pc) {
			continue
		}
		if err := c.ovnClient.DelChain(pc); err != nil {
			klog.Errorf("failed to delete port chain %s, %v", pc, err)
			return err
		}
	}
	return nil
}
