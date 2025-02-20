# Big Query 학습 내용 정리

### BigQuery 사용하기

```
dependencies {
    implementation platform('com.google.cloud:libraries-bom:26.54.0')
    implementation 'com.google.cloud:google-cloud-bigquery'
}
```

### BigQuery 클라이언트 연결 방식
1. 환경 변수 사용

```java
BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
```

2. json 파일 사용

```java
String jsonPath = "json 파일 경로";
BigQuery bigquery = BigQueryOptions.newBuilder()
        .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(jsonPath)))
        .build()
        .getService();
```