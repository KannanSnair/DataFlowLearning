package com.dataflow.examples;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {

    	
    	final List<String> LINES = Arrays.asList("1/5/99,Shoes,1200,AMEX,Netherlands",
    			"2/6/99,jacket,1300,mastercard,Unitedstates",
    			"3/6/99,phone,2000,Visa,Unitedstates",
    			"4/6/99,Books,200,Diners,Ireland");

    	PipelineOptions options = PipelineOptionsFactory.create();
    	//options.setRunner(DirectRunner.class);//(DirectRunner.class);
    	Pipeline pipeline = Pipeline.create(options);
    	
    	
    	//InputStream inputStream = new FileInputStream(new File(""));
    	System.out.print("testing by kannan!!");
//----------------------------------------------------------------------
    	// Transform Function - PARDO example
    	
/*		  pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
		  .apply("Print before transformation: ", ParDo.of(new PrintToConsoleFn()))
		  .apply(ParDo.of(new ExtractPaymentTypeFn()))
		  .apply("Print after transformation: ", ParDo.of(new PrintToConsoleFn()));   */
//----------------------------------------------------------------------		 
		  
//----------------------------------------------------------------------		  
    	// Transform Function - MapElements/Flatelement example
		  pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
		  .apply("Print before transformation: ", MapElements.via(new SimpleFunction<String, String>() {
			  
			  @Override
			  public String apply(String input) {
				  
				  System.out.println(input);
				  return input;
			  }
		}))
		  .apply("Print after transformation: ", FlatMapElements.into(TypeDescriptors.strings())
				  .via(line -> Collections.singleton(line.split(",")[3])))
		  .apply(ParDo.of(new PrintToConsoleFn()));   
//----------------------------------------------------------------------		  

//----------------------------------------------------------------------	*/
    	//Count.perElement() example
    	pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
		  .apply("Print before transformation: ", ParDo.of(new PrintToConsoleFn()))
		  .apply("Extract Payment Types: ", FlatMapElements.into(TypeDescriptors.strings())
				  .via(line -> Collections.singleton(line.split(",")[3])))
		  .apply("Aggreation count: ", Count.perElement())
		  .apply("Format output: ", MapElements
				  .into(TypeDescriptors.strings())
				  .via(hm -> hm.getKey() + ":" + hm.getValue()))
		  .apply("printextracted output: ", ParDo.of(new PrintToConsoleFn()));
    	
//----------------------------------------------------------------------	    	
		  pipeline.run().waitUntilFinish();
    	
    }
    
    public static class ExtractPaymentTypeFn extends DoFn<String, String> {
    	
    	@ProcessElement
    	public void processElement(ProcessContext c) {
    		
    		String[] tokens = c.element().split(",");
    		if(tokens.length >=4) {
    			c.output(tokens[3]);
    		}
    	}    	
    }
    public static class PrintToConsoleFn extends DoFn<String, String> {
    	
    	@ProcessElement
    	public void processElement(ProcessContext c) {
    		
    		System.out.println(c.element());
    		c.output(c.element());
    	}
    }
    public static class PrintToConsoleFn1 extends DoFn<HashMap<String, Long>, Long> {
    	
    	@ProcessElement
    	public void processElement(ProcessContext c) {
    		
    		System.out.println(c.element());
    		c.output(c.element().get(""));
    	}
    }
}
