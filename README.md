# airflow_tesst
Repositorio para compatir la prueba técnica para TalentPitch

Tener en cuenta los siguientes pasos para ejecutar el flujo
1. Instalar docker a partir del siguiente link https://docs.docker.com/get-docker/
2. Instalar docker-compose de acuerdo al sistema operativo desde el link https://docs.docker.com/compose/install/
3. Copiar el archivo ".yaml" del siguente link https://airflow.apache.org/docs/apache-airflow/2.6.0/docker-compose.yaml
4. pegar el contenido en un archivo ".yaml" en su computador
5. ejecutar el ".yaml" con la sentencia "docker-compose up airflow-init" desde la ubicacion del ".yaml"
6. Al terminar la instalación, ya puede abrir airflow en su navegador http://localhost:8080/home
7. En la carpeta "dags" pegar el dag desarrollado ubicado en https://github.com/gagarb/airflow_tesst/tree/main/airflow_test
8. habilite el dag en aiflow y revise los logs para evidenciar la ejecución del proceso
