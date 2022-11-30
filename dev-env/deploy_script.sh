
#this script runs inside the container. 
#It clones the repository and then checkout for the specific commit if added. 
#After, it runs the cont_execute script in the brie repository. 

git clone git@github.com:eeg-sense/containerise-cloud-environment.git

git fetch

echo "commit_version $COMMIT_HASH"
echo "db_port $DB_PORT"
echo "http_port $HTTP_PORT"

#reate a DB in the container and then copy all the data from prod to the DB
[[ ! -z "$COMMIT_HASH" ]] && git checkout $COMMIT_HASH
        
python3 cont_execute.py