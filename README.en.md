# 1993 CLCD Land Cover Analysis

The current `main.py` runs CLCD analysis through a Master/Slave task flow. It splits a drawn polygon, an imported GeoJSON polygon, or a selected administrative area by province, city, or county according to `split_level`, then distributes raster cropping and polygonization to Slave nodes through a dedicated RabbitMQ queue. GeoJSON files may contain multiple polygon features in WGS84 coordinates. The Master merges the raster and vector chunks. The Web page polls task progress and shows class areas and shares after completion.

See [README.md](README.md) for data paths, shared output storage, node roles, calculation rules, API, and local test instructions. In `role=all`, local background calculation remains available when RabbitMQ is unavailable.
