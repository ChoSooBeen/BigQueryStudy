package org.example;

/**
 * Big Query Test
 *
 * @author : chosoobeen
 * @date : 2025-02-19
 */


import com.google.cloud.bigquery.*;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import java.io.FileInputStream;
import java.io.IOException;


public class Main {
    static BigQuery bigquery = null;
    public static void main(String[] args) {

        //서비스 계정 JSON 파일 경로
        String jsonPath = "D:\\BigQueryTest\\sodium-keel-451407-g2-37e22415e6f0.json";

        // BigQuery 클라이언트 생성
        try {
            bigquery = BigQueryOptions.newBuilder()
                    .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(jsonPath)))
                    .build()
                    .getService();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // 연결 테스트
        System.out.println("BigQuery 연결 성공! \n프로젝트 ID: " + bigquery.getOptions().getProjectId());

        // 쿼리 생성
        String projectId = bigquery.getOptions().getProjectId();
        String datasetName = "test_dataset_001";
        String tableName = "test_table_001";

        //현재 프로젝트의 데이터 세트 목록 조회
        for(Dataset dataset : bigquery.listDatasets().iterateAll()) {
            System.out.println(dataset.getDatasetId().getDataset());
        }

        //현재 프로젝트의 한 데이터 세트 속 테이블 목록 조회
        for (Table table : bigquery.listTables(datasetName).iterateAll()) {
            System.out.println(table.getTableId().getTable());
        }

        String query = " SELECT * "
                     + " FROM `" + projectId + "."+ datasetName + "." + tableName + "` ";

        System.out.println("실행할 Query : " + query);

        queryDryRun(query);
        queryDryRun("SELECT *  FROM ` sodium-keel-451407-g2.test_dataset_001.test_table_001` "); //오류 테스트
        simpleQuery(query);
        //queryBatch(query);
    }

    /*
     * @2025.02.19 dry-run
     */
    public static void queryDryRun(String query) {
        try {
            //환경 변수로 설정된 인증 정보를 사용할 경우
            //BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            //setDryRun()
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setDryRun(true).setUseQueryCache(false).build();

            Job job = bigquery.create(JobInfo.of(queryConfig));
            JobStatistics.QueryStatistics statistics = job.getStatistics();

            // Dry-run 결과 출력
            System.out.println("\n쿼리 Dry Run 성공!");
            System.out.println("총 처리 바이트 (TotalBytesProcessed): " + statistics.getTotalBytesProcessed() + " bytes\n");

        } catch (BigQueryException e) {
            System.out.println("Query is invalid or another error occurred: \n" + e.toString() + "\n");
        }
    }

    /*
     * @2025.02.20 기본 쿼리 실행
     */
    public static void simpleQuery(String query) {
        try {
            //캐시를 사용한 경우 처리되거나 청구된 바이트 없을 수 있다.
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setUseQueryCache(false).build();

            // 쿼리 실행 및 Job 생성
            Job job = bigquery.create(JobInfo.of(queryConfig));

            // Job이 완료될 때까지 대기
            job = job.waitFor();

            if (job == null) {
                System.out.println("Before BigQuery");
                return;
            } else if (job.getStatus().getError() != null) {
                System.out.println("Query not performed \n" + job.getStatus().getError().toString());
                return;
            }

            //작업(Job) 정보 가져오기
            JobStatistics.QueryStatistics stats = job.getStatistics();
            System.out.println("쿼리 실행 완료!");
            System.out.println("작업 ID: " + job.getJobId().getJob());
            System.out.println("사용자: " + job.getUserEmail());
            System.out.println("만든 시간: " + job.getStatistics().getCreationTime()); //stats 가 아닌 JobStatistics 슈퍼 클래스에 존재하는 메소드임
            System.out.println("시작 시간: " + job.getStatistics().getStartTime());
            System.out.println("종료 시간: " + job.getStatistics().getEndTime());
            System.out.println("처리한 바이트: " + stats.getTotalBytesProcessed() + " bytes");
            System.out.println("청구된 바이트: " + stats.getTotalBytesBilled() + " bytes");
            System.out.println("슬롯 밀리초: " + job.getStatistics().getTotalSlotMs() + " ms");
            System.out.println("legacy SQL 사용: " + queryConfig.useLegacySql());
            System.out.println("캐시 사용 여부: " + stats.getCacheHit());

            //쿼리 결과 가져오기
            TableResult result = bigquery.query(queryConfig);
            System.out.println("\n쿼리 수행 결과");
            System.out.println(result.toString());

        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Query did not run \n" + e.toString());
        }
    }

    /*
     * @2025.02.20 일괄 쿼리 실행
     */
    public static void queryBatch(String query) {
        try {
            //setPriority()
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setPriority(QueryJobConfiguration.Priority.BATCH).build();

            TableResult results = bigquery.query(queryConfig);
            System.out.println("\n쿼리 batch 성공!");

            System.out.println(results.toString());
            System.out.println("Query batch performed successfully.");

        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Query not performed \n" + e.toString());
        }
    }
}