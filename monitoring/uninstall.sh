helm uninstall prometheus -n monitoring &
helm uninstall  grafana -n monitoring 
sleep 5
kubectl delete -f prometheus-pvc-local.yaml 
kubectl delete -f prometheus-pv-local.yaml 
sleep 5
helm uninstall prometheus-adapter -n monitoring &
helm uninstall    metrics-server -n monitoring 
sleep 5

sleep 5

kubectl delete -f grafana-pvc-local.yaml 
kubectl delete -f grafana-pv-local.yaml 

current_path=$(pwd)
# cd ${current_path}/monitoring/prometheus
# rm -rf *

# cd ${current_path}/monitoring/grafana
# rm -rf *

# cd $pwd