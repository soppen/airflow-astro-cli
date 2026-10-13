# Devcontainer setup

Devcontainer for developing data pipelines for Airflow. 

Using Python with uv, Astro CLI and a clickhouse database.

Go to `src/astro` to start the airflow dev service 

```
cd src/astro
astro dev start
```

clickhouse available at `localhost:8123`, checkout `localhost:8123/play` to fiddle with the tables there.

