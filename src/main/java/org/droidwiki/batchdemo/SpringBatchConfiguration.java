package org.droidwiki.batchdemo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.SimpleJob;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.AbstractStep;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfiguration {
    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(1);

        return taskExecutor;
    }

    @Bean
    public JpaTransactionManager jpaTransactionManager(DataSource dataSource) {
        JpaTransactionManager jpaTransactionManager = new JpaTransactionManager();
        jpaTransactionManager.setDataSource(dataSource);
        return jpaTransactionManager;
    }

    @Bean
    public BatchConfigurer batchConfigurer(TaskExecutor taskExecutor, JpaTransactionManager jpaTransactionManager) {
        return new AsyncBatchConfigurer(taskExecutor, jpaTransactionManager);
    }

    @Bean
    public Job demoJob(JobRepository jobRepository, Step demoStep) {
        SimpleJob job = new SimpleJob("demoJob");
        job.setJobRepository(jobRepository);
        job.addStep(demoStep);

        return job;
    }

    @Bean
    public Step demoStep(JobRepository jobRepository) {
        AbstractStep step = new AbstractStep() {
            @Override
            protected void doExecute(StepExecution stepExecution) throws Exception {
                System.out.println("Executing demoStep");
                Thread.sleep(5000);
            }
        };
        step.setJobRepository(jobRepository);

        return step;
    }

    private class AsyncBatchConfigurer extends DefaultBatchConfigurer {
        private final TaskExecutor taskExecutor;
        private final JpaTransactionManager jpaTransactionManager;

        AsyncBatchConfigurer(TaskExecutor taskExecutor, JpaTransactionManager jpaTransactionManager) {
            this.taskExecutor = taskExecutor;
            this.jpaTransactionManager = jpaTransactionManager;
        }

        @Override
        public PlatformTransactionManager getTransactionManager() {
            return jpaTransactionManager;
        }

        @Override
        protected JobRepository createJobRepository() throws Exception {
            JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
            factory.setDataSource(jpaTransactionManager.getDataSource());
            factory.setTransactionManager(jpaTransactionManager);
            factory.setIsolationLevelForCreate("ISOLATION_REPEATABLE_READ");
            factory.afterPropertiesSet();

            return factory.getObject();
        }

        @Override
        protected JobLauncher createJobLauncher() throws Exception {
            SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
            jobLauncher.setJobRepository(getJobRepository());
            jobLauncher.setTaskExecutor(taskExecutor);
            jobLauncher.afterPropertiesSet();

            return jobLauncher;
        }
    }
}
