#! /bin/bash

#validate user input
if [ -z "$1" ]
    then
        usage
        echo "Storage account name must be provided."
        exit 137
fi

if [ -z "$2" ]
    then
        usage
        echo "Storage account key must be provided."
        exit 138
fi

STORAGEACCOUNTNAME=$1
if [[ $1 == *blob.core.windows.net* ]]; then
    echo "Extracting storage account name from $1"
    STORAGEACCOUNTNAME=$(echo $1 | cut -d'.' -f 1)
fi
echo STORAGE ACCOUNT IS: $STORAGEACCOUNTNAME

STORAGEACCOUNTKEY=$2

#validate storage account credentials
echo "Validate storage account creds:"
CREDS_VALIDATION=$(echo -e "from azure.storage.blob import BlobService\nvalid=True\ntry:\n\tblob_service = BlobService(account_name='$STORAGEACCOUNTNAME', account_key='$STORAGEACCOUNTKEY')\n\tblob_service.get_blob_service_properties()\nexcept Exception as e:\n\tvalid=False\nprint valid"| sudo python)
if [[ $CREDS_VALIDATION == "False" ]]; then
    echo "Invalid Credentials provided for storage account"
    exit 141
else
    echo "Successfully validated storage account credentials."
fi

#Encrypt storage account key
CERT=$(sudo grep -R --include="*.crt" "HDInsight.Production.Encryption.Cert" /var/lib/waagent/ | cut -d ":" -f 1)
echo $2 | sudo openssl cms -encrypt -outform PEM -out storagekey.txt $CERT
if (( $? )); then
    echo "Could not encrypt storage account key"
    exit 139
fi
STORAGEACCOUNTKEY=$(echo -e "import re\n\nfile = open('storagekey.txt', 'r')\nfor line in file.read().splitlines():\n\tif '-----BEGIN CMS-----' in line or '-----END CMS-----' in line:\n\t\tcontinue\n\telse:\n\t\tprint line\nfile.close()" | sudo python)
STORAGEACCOUNTKEY=$(echo $STORAGEACCOUNTKEY | tr -d ' ')
echo "STORAGEACCOUNTKEY=$STORAGEACCOUNTKEY"
if [ -z "$STORAGEACCOUNTKEY" ];
    then
        echo "Storage account key could not be stripped off header values form encrypted key"
        exit 140
fi
sudo rm storagekey.txt

#Validate storage account region
#1. get default SA account name
CORESITECONTENT=$(sudo bash $AMBARICONFIGS_SH -u $USERID -p $PASSWD get $ACTIVEAMBARIHOST  $CLUSTERNAME core-site )
echo $CORESITECONTENT > coresiteread.txt
DEFAULTSANAME=$(echo -e "import re\nfile = open('coresiteread.txt','r').read()\nfor line in file.splitlines():\n\tif 'fs.defaultFS' in line:\n\t\tm = re.search('wasb://(.+?).blob.core.windows.net', line).group(1)\n\t\tif m:\n\t\t\tprint m"| sudo python)
DEFAULTSANAME=$(echo $DEFAULTSANAME | cut -d '@' -f 2)
#2. get default SA account key
#2. decrypt default SA key
#3. determine region from decrypted SA key
#4. determine region for user's input SA key
#5. if default region == user SA region ? continue : exit
sudo rm -f coresiteread.txt
AMBARICONFIGS_SH=/var/lib/ambari-server/resources/scripts/configs.sh
PORT=8080

ACTIVEAMBARIHOST=headnodehost

usage() {
    echo ""
    echo "Usage: sudo -E bash add-storage-account.sh <storage-account-name> <storage-account-key>";
    echo "This script does NOT require Ambari username and password";
    exit 132;
}

checkHostNameAndSetClusterName() {
    fullHostName=$(hostname -f)
    echo "fullHostName=$fullHostName"
    if [[ $fullHostName != headnode0* && $fullHostName != hn0* ]]; then
        echo "$fullHostName is not headnode 0. This script has to be run on headnode0."
        exit 0
    fi
    CLUSTERNAME=$(sed -n -e 's/.*\.\(.*\)-ssh.*/\1/p' <<< $fullHostName)
    if [ -z "$CLUSTERNAME" ]; then
        CLUSTERNAME=$(echo -e "import hdinsight_common.ClusterManifestParser as ClusterManifestParser\nprint ClusterManifestParser.parse_local_manifest().deployment.cluster_name" | python)
        if [ $? -ne 0 ]; then
            echo "[ERROR] Cannot determine cluster name. Exiting!"
            exit 133
        fi
    fi
    echo "Cluster Name=$CLUSTERNAME"
}

validateUsernameAndPassword() {
    coreSiteContent=$(bash $AMBARICONFIGS_SH -u $USERID -p $PASSWD get $ACTIVEAMBARIHOST $CLUSTERNAME core-site)
    echo $coreSiteContent
    if [[ $coreSiteContent == *"[ERROR]"* && $coreSiteContent == *"Bad credentials"* ]]; then
        echo "[ERROR] Username and password are invalid. Exiting!"
        exit 134
    fi
}

updateAmbariConfigs() {
    updateResult=$(bash $AMBARICONFIGS_SH -u $USERID -p $PASSWD set $ACTIVEAMBARIHOST $CLUSTERNAME core-site "fs.azure.account.key.$STORAGEACCOUNTNAME.blob.core.windows.net" "$STORAGEACCOUNTKEY")
    
    if [[ $updateResult != *"Tag:version"* ]] && [[ $updateResult == *"[ERROR]"* ]]; then
        echo "[ERROR] Failed to update core-site. Exiting!"
        echo $updateResult
        exit 135
    fi
    echo "Added property: 'fs.azure.account.key.$STORAGEACCOUNTNAME.blob.core.windows.net' with storage account key"

    updateResult=$(bash $AMBARICONFIGS_SH -u $USERID -p $PASSWD set $ACTIVEAMBARIHOST $CLUSTERNAME core-site "fs.azure.account.keyprovider.$STORAGEACCOUNTNAME.blob.core.windows.net" "org.apache.hadoop.fs.azure.SimpleKeyProvider")
    
    if [[ $updateResult != *"Tag:version"* ]] && [[ $updateResult == *"[ERROR]"* ]]; then
        echo "[ERROR] Failed to update core-site. Exiting!"
        echo $updateResult
        exit 135
    fi
    echo "Added property: 'fs.azure.account.keyprovider.$STORAGEACCOUNTNAME.blob.core.windows.net':org.apache.hadoop.fs.azure.SimpleKeyProvider "
}

stopServiceViaRest() {
    if [ -z "$1" ]; then
        echo "Need service name to stop service"
        exit 136
    fi
    SERVICENAME=$1
    echo "Stopping $SERVICENAME"
    curl -u $USERID:$PASSWD -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Stop Service for adding storage account"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' http://$ACTIVEAMBARIHOST:$PORT/api/v1/clusters/$CLUSTERNAME/services/$SERVICENAME
}

startServiceViaRest() {
    if [ -z "$1" ]; then
        echo "Need service name to start service"
        exit 136
    fi
    sleep 2
    SERVICENAME=$1
    echo "Starting $SERVICENAME"
    startResult=$(curl -u $USERID:$PASSWD -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Start Service after adding storage account"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' http://$ACTIVEAMBARIHOST:$PORT/api/v1/clusters/$CLUSTERNAME/services/$SERVICENAME)
    if [[ $startResult == *"500 Server Error"* || $startResult == *"internal system exception occurred"* ]]; then
        sleep 60
        echo "Retry starting $SERVICENAME"
        startResult=$(curl -u $USERID:$PASSWD -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Start Service after adding storage account"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' http://$ACTIVEAMBARIHOST:$PORT/api/v1/clusters/$CLUSTERNAME/services/$SERVICENAME)
    fi
    echo $startResult
}

##############################
if [ "$(id -u)" != "0" ]; then
    echo "[ERROR] The script has to be run as root."
    usage
fi

USERID=$(echo -e "import hdinsight_common.Constants as Constants\nprint Constants.AMBARI_WATCHDOG_USERNAME" | python)

echo "USERID=$USERID"

PASSWD=$(echo -e "import hdinsight_common.ClusterManifestParser as ClusterManifestParser\nimport hdinsight_common.Constants as Constants\nimport base64\nbase64pwd = ClusterManifestParser.parse_local_manifest().ambari_users.usersmap[Constants.AMBARI_WATCHDOG_USERNAME].password\nprint base64.b64decode(base64pwd)" | python)

checkHostNameAndSetClusterName
validateUsernameAndPassword
echo "***************************UPDATING AMBARI CONFIG**************************"
updateAmbariConfigs
echo "***************************UPDATED AMBARI CONFIG**************************"

stopServiceViaRest OOZIE
stopServiceViaRest YARN
stopServiceViaRest MAPREDUCE2
stopServiceViaRest HDFS


startServiceViaRest HDFS
startServiceViaRest MAPREDUCE2
startServiceViaRest YARN
startServiceViaRest OOZIE
