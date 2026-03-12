#/bin/bash
echo "Starting..."
(cd ../pkg/recommender && make docker-build) &&
(cd ../pkg/recommender && make docker-push) &&
kubectl delete -f recommender-deployment.yaml &&
kubectl apply -f recommender-deployment.yaml &&
sleep 70
pod_name=$(kubectl get pod -l app=vpa-recommender -A -o name)
check_errors=$(kubectl logs -n kube-system  $pod_name)
echo "Checking output"
echo $pod_name
echo ""
if kubectl logs -n kube-system  $pod_name | grep -iq "bad_data"; then
  echo "ALARM: Fail detected! -> $(kubectl logs -n kube-system  $pod_name) | grep -i "bad_data" -A 2 -B 3)";
  exit 1
fi

echo "Executing logs"

kubectl logs -n kube-system -f $pod_name | grep "SLA Data for demo Ag" -B 1 -A 100
