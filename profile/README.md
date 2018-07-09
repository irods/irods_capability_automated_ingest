=== log profile ===

`--profile` `--profile_filename <profile filename>` `--profile_level INFO`

=== elasticsearch ===

`config/elasticsearch.yml`

add

```
http.cors.enabled: true
http.cors.allow-origin: "*"
```

```
curl -XPUT <elasticsearch host>:9200/icaiprofile -H "Content-Type: application/json" -d '
{
  "mappings": {
    "document": {
      "properties": {
	 "hostname": {
	   "type": "keyword"
	 }
      }
    }
  }
}'
```
=== ingest ===

```
pip install elasticsearch
```

```
python profile.py <profile filename> <additional properties> <elasticsearch host> <index>
```

=== visualize ===

firefox profile.html

	

