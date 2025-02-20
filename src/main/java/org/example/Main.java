package org.example;

/**
 * Big Query Test
 *
 * @author : chosoobeen
 * @date : 2025-02-19
 */


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import java.io.FileInputStream;
import java.io.IOException;
import com.google.cloud.bigquery.TableResult;


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
        String query = " SELECT * "
                     + " FROM `" + projectId + "."+ datasetName + "." + tableName + "` "; //백틱과 프로젝트ID 사이에 띄어쓰기 되어있으면 오류

        System.out.println("실행할 Query : " + query);

        //queryDryRun(query);
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
            System.out.println("총 처리 바이트 (TotalBytesProcessed): " + statistics.getTotalBytesProcessed() + " bytes");
            System.out.println("총 청구 바이트 (TotalBytesBilled): " + statistics.getTotalBytesBilled() + " bytes");

        } catch (BigQueryException e) {
            System.out.println("Query not performed \n" + e.toString());
        }
    }

    /*
     * @2025.02.20 기본 쿼리 실행
     */
    public static void simpleQuery(String query) {
        try {

            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

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
            System.out.println("총 처리 바이트: " + stats.getTotalBytesProcessed() + " bytes");
            System.out.println("총 청구 바이트: " + stats.getTotalBytesBilled() + " bytes");

            //없는 메소드인듯?
            //System.out.println("쿼리 실행 시간: " + stats.getExecutionTime() + " ms");
            //System.out.println("쿼리 대기 시간: " + stats.getPendingTime() + " ms");
            System.out.println("캐시 사용 여부: " + stats.getCacheHit());
            System.out.println("DML 영향 받은 행 수: " + stats.getNumDmlAffectedRows());

            //쿼리 결과 가져오기
            TableResult result = bigquery.query(queryConfig);
            System.out.println("\n쿼리 simple 성공!");
            System.out.println(result.toString());
            System.out.println("Query ran successfully");
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
            //results.iterateAll().forEach(row -> row.forEach(val -> System.out.printf("%s,", val.toString())));

            System.out.println(results.toString());
            System.out.println("Query batch performed successfully.");

        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Query not performed \n" + e.toString());
        }
    }
}