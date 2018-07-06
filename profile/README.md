=== log profile ===

`--profile` `--profile_filename <profile filename>` `--profile_level INFO`

=== elasticsearch ===

`config/elasticsearch.yml`

add

```
http.cors.enabled: true
http.cors.allow-origin: "*"
```

=== ingest ===

```
pip install elasticsearch
```

```
python profile.py <profile filename> <index> [ --elasticsearch_host <elasticsearch host> ] [ --additional_key <additional properties> ]
```

=== visualize ===

firefox profile.html

	

