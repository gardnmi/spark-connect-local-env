export SPARK_VERSION

start_connect_server:
	@echo "Starting connect server"
	/opt/spark/sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:$(SPARK_VERSION)

stop_connect_server:
	@echo "Stopping connect server"
	/opt/spark/sbin/stop-connect-server.sh --packages org.apache.spark:spark-connect_2.12:$(SPARK_VERSION)