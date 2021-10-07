package com.dataflow.examples;

import java.util.Collections;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptors;

public class FileReadClass {

	
	public static void main(String[] args) {

		String CSV_HEADER = "Date,Product,Price,Card,Country";
		
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline pipeline = Pipeline.create(options);
		
		pipeline.apply("ReadLine", TextIO.read().from("src/main/resources/SalesFile.csv"))
		.apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))  //ParDo.of(new FilterHeaderFn(CSV_HEADER))
		.apply("ExtractPaymentTypes", FlatMapElements.into(TypeDescriptors.strings())
				.via(p -> Collections.singletonList(p.split(",")[3])))
		.apply("", Count.perElement());
				
						

	}

	private static class FilterHeaderFn extends DoFn<String, String> {
		
		private final String header;
		public FilterHeaderFn(String header) {
			
			this.header = header;
		}
		
		@ProcessElement
		public void processElement(ProcessContext c) {
			
			String row = c.element();
			if(!row.isEmpty() || !row.equals(this.header)) {
				c.output(row);
			}
		}
	}
}
