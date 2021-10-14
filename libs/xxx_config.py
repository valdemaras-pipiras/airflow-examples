from airflow.models import Variable

# provider wide config
config = {
  "providerId": Variable.get("_provider_id"),
  "providerTag": "XXX",
  "providerName": "xxx.lt",
  "providerDesc": "XXX",
  "serviceProvider": "YYYYYYY",
  "apiUrl": Variable.get("api_address"),
  "apiUser": Variable.get("api_user"),
  "apiPassword": Variable.get("api_password"),
  "srcPath": Variable.get("import_src_path"),
  "dstMuxPath": Variable.get("mux_dst_path"),
  "videoFileStruct": "*_.mov",
  "audioFileStruct": "*.aac",
  "subtitleFileStruct": "*.stl",
  "allowedLangTracks": Variable.get("mux_allowed_lang_tracks").split(','),
  "vantageSrcPath": Variable.get("code_src_path"),
  "vantageDstPath": Variable.get("code_dst_path"),
  "srcCodePath": Variable.get("code_src_path"),
  "dstCodePath": Variable.get("code_dst_path"),
  "vantageQueueSize": Variable.get("code_queue_size"),
  "vantageUri": Variable.get("code_uri"),
  "vantageS2WorkflowUuid": Variable.get("code_s2_workflow_uuid"),
  "userAgent": Variable.get("app_user_agent"),
  "srcAdi2Path": Variable.get("adi2_src_path"),
  "dstAdi2Path": Variable.get("adi2_dst_path")
}
