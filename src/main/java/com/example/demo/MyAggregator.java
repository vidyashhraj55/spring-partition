package com.example.demo;

import java.util.Collection;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.partition.support.DefaultStepExecutionAggregator;
import org.springframework.batch.item.ExecutionContext;

public class MyAggregator extends DefaultStepExecutionAggregator {

	@Override
	public void aggregate(StepExecution result, Collection<StepExecution> executions) {
		// TODO Auto-generated method stub
		super.aggregate(result, executions);
		System.out.println("Aggregated--------------");
		for (StepExecution stepExecution : executions) 
		{
			ExecutionContext executionContext1 = stepExecution.getExecutionContext();
			Object x = executionContext1.get("x");
			System.out.println("x="+x);
//			Long id = stepExecution.getId();
//			
//			StepExecution update = jobExplorer.getStepExecution(stepExecution.getJobExecutionId(), id);
//			ExecutionContext executionContext = update.getExecutionContext();
//			int int1 = executionContext.getInt("x");
//			System.out.println("GOT "+int1);
		}
	}
}