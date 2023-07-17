kubectl get pod -n sparglim |grep sparglim | awk '{print $(1)}' |xargs kubectl delete pod -n sparglim
