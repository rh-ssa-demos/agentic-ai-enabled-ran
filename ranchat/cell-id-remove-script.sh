# This script is used to remove or clear off anomaly for the cell id from the ranchat database
# It will send a request to the ranchat api to clear off the anomaly for the cell id
# It will loop through the cell ids from the range we define and send a request to the ranchat api to clear off the anomaly for the cell id
for id in {30..39}
do
  echo "Sending request for Cell ID: $id"
  curl -k -X POST https://ranchat-ai-cloud-ran-genai.apps.acmhub.dinesh154.dfw.ocp.run/api/ran \
       -H "Content-Type: application/json" \
       -d "{
            \"cell_id\": $id,
            \"anomaly_type\": \"UEs Spike/Drop\",
            \"anomaly_value\": \"80.0 UEs\",
            \"threshold_value\": \"50%\",
            \"action\": \"Balance DU assignment across available CU instances. Refer to Baicells documentation (Section 7, Issue: Inconsistent DU-CU Communication)\"
           }"
  echo -e "\n---"
done