package com.example.demo;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.step.builder.PartitionStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;


@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DataSource dataSource;

   
    @Bean
    public Partitioner rangePartitioner() {
    	RangePartitioner rangePartitioner = new RangePartitioner();
        return rangePartitioner;
    }
    // tag::listener[]

    @Bean
    public JobExecutionListener listener() {
        return new JobCompletionNotificationListener(new JdbcTemplate(dataSource));
    }
    
    @Bean
    public MyAggregator myAggregator() {
        return new MyAggregator();
    }

    // end::listener[]

    // tag::jobstep[]
    @Bean
    public Job importUserJob() {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener())
                .flow(masterStep())
                .end()
                .build();
    }
    
    @Bean
	public TaskExecutor taskExecutor() {
    	ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(7);
        executor.setMaxPoolSize(42);
        executor.setQueueCapacity(11);
        executor.setThreadNamePrefix("MyExecutor-");
        executor.initialize();
		return executor;
	}
    @Bean
    public org.springframework.batch.core.partition.PartitionHandler partitionHandler()
    {
    	TaskExecutorPartitionHandler taskExecutorPartitionHandler = new TaskExecutorPartitionHandler();
    	taskExecutorPartitionHandler.setTaskExecutor( taskExecutor());
    	taskExecutorPartitionHandler.setStep(slaveStep());
    	taskExecutorPartitionHandler.setGridSize(10);
    	return taskExecutorPartitionHandler;
    }

    @Bean
    public Step masterStep() {
    	StepBuilder stepBuilder = stepBuilderFactory.get("masterstep");
    	Partitioner rangePartitioner = rangePartitioner();
    	//rangePartitioner.partition(10);
    	PartitionStepBuilder partitionStepBuilder = stepBuilder.partitioner("slavestep", rangePartitioner);
    	partitionStepBuilder.partitionHandler(partitionHandler());
    	partitionStepBuilder.aggregator(myAggregator());
        return partitionStepBuilder
                .build();
        
        //.faultTolerant().retry(BaseSystemException.class).retryLimit(3)
    }
    
    @Bean
    public Step slaveStep() {
        return stepBuilderFactory.get("slavestep").tasklet(new Tasklet(){

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				
				StepContext stepContext = chunkContext.getStepContext();
				StepExecution stepExecution = stepContext.getStepExecution();
				
				ExecutionContext executionContext = stepExecution.getExecutionContext();
				Map<String, Object> stepExecutionContext = chunkContext.getStepContext().getStepExecutionContext();
				Object fromId = stepExecutionContext.get("fromId");
				Object toId = stepExecutionContext.get("toId");
				Object name = stepExecutionContext.get("name");
		         
				System.out.println("executing tasklet fromId="+fromId+",toId="+toId+",name="+name+",thread.name="+Thread.currentThread().getName());
				//stepExecutionContext.put("x", fromId);//wont work
				executionContext.put("x", fromId);//will work
				return RepeatStatus.FINISHED;
			}}).build();
                
        
       
    }
}
