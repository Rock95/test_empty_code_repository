#! /bin/bash
#****************************************************************#
# ScriptName: hdfs-minicluster.sh
#***************************************************************#
HADOOP_HOME=${HADOOP_HOME}
SHELL_NAME=$0
SHELL_LOG="/var/log/mock-hdfs-cluster.log"
HDFS_TEST_CONF_DIR="/tmp"

NN_PORT=20300
DN_NUM=3

LOCK_FILE="/tmp/${SHELL_NAME}.lock"
PROG=$(basename "$0")

function shell_log(){
    LOG_INFO=$1
    echo "$(date "+%Y-%m-%d") $(date "+%H:%M:%S") : ${SHELL_NAME} : ${LOG_INFO}" | tee -ai ${SHELL_LOG} 2>&1
}

function usage(){
        echo "Usage: ${PROG} (subcommand)
    Subcommands:
        help              Prints this message
        start             Start hdfs minicluster
                          HADOOP_HOME=/xx/xx sh $0 start
        status            Get hdfs minicluster status
        stop              Stop hdfs minicluster
" 1>&2
}

function pre_check(){
    if [[ ! ${HADOOP_HOME} ]] || [[ ! -f ${HADOOP_HOME}/bin/hadoop ]];then
        shell_log "Can't find Hadoop Home"
        exit 1
    fi

    HDFS_TEST_JAR=$(find $HADOOP_HOME/share/hadoop/hdfs/ -name "hadoop-hdfs-*-tests.jar" | head -n 1)

    if [[ ! -f ${HDFS_TEST_JAR} ]];then
        shell_log "Can't find hadoop-hdfs-*-tests.jar"
        exit 1
    fi
}

hadoop=" ${HADOOP_HOME}/bin/hadoop "


function status_minicluster(){
    cluster_pid=$(ps -ef | grep org.apache.hadoop.test.MiniDFSClusterManager | grep -v grep | awk '{print $2}')
    if [[ "X${cluster_pid}" == "X" ]];then
        shell_log "hdfs minicluster is stop."
    else
        shell_log "hdfs minicluster is running. pid ---> ${cluster_pid}"
        namenode_port=$(ps -ef | grep org.apache.hadoop.test.MiniDFSClusterManager | grep -v grep | awk -F"-nnport " '{print $2}' | awk '{print $1}')
        shell_log "nnport ---> ${namenode_port}, hdfs url is: hdfs://localhost:${namenode_port}/"
    fi
    return ${cluster_pid} 
}

function stop_minicluster(){
    status_minicluster
    if [[ x"${cluster_pid}x" != "xx" ]];then
        stop_cmd="kill -9 ${cluster_pid}"
        shell_log "${stop_cmd}"
        eval "${stop_cmd}"
        status_minicluster
    fi
    rm -f ${LOCK_FILE}
}

function start_minicluster(){
    if [[ -f ${LOCK_FILE} ]];then
        shell_log "stop hdfs minicluster first."
        status_minicluster
        exit 0
    fi
    rm -f $HDFS_TEST_CONF_DIR/core-site.xml 
    ${hadoop} jar ${HDFS_TEST_JAR} \
    org.apache.hadoop.test.MiniDFSClusterManager \
    -format \
    -nnport ${NN_PORT} \
    -datanodes ${DN_NUM} \
    -writeConfig $HDFS_TEST_CONF_DIR/core-site.xml \
    > ./hdfs-minicluster.out 2>&1 &

    local mini_cluster_pid=$!
    for i in {1..15}; do
        shell_log "Waiting for DFS cluster, attempt $i of 15"
        [ -f ${HDFS_TEST_CONF_DIR}/core-site.xml ] && break;
        sleep 2
    done
    if [ ! -f ${HDFS_TEST_CONF_DIR}/core-site.xml ]; then
        shell_log "Cluster did not come up in 30s"
        kill -9 ${mini_cluster_pid}
        exit 1
    fi
    touch ${LOCK_FILE}
    status_minicluster
}

function hdfs_info(){
    shell_log "TODO"
}

main() {

    subcommand="$1"
    if [ x"${subcommand}x" == "xx" ]; then
        subcommand="help"
    else
        shift # past sub-command
    fi
    case $subcommand in
        help)
            usage
            ;;
        start)
            pre_check
            start_minicluster
            ;;
        status)
            status_minicluster
            ;;
        stop)
            stop_minicluster
            ;;
        *)
            # unknown option
            usage
            exit 1
            ;;
    esac
    exit 0
}

main "$@"
exit 0