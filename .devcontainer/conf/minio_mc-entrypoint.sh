#/bin/bash  
  
  
/usr/bin/mc config host add local http://minio:9000 ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY}  
/usr/bin/mc mb local/delta-lake
exit 0