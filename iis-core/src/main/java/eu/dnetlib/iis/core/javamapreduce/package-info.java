/**
 * Classes that are needed by Java-based map-reduce workflow nodes.
 * <p/>
 * 
 * NOTE: When you want to see what properties have to be given in Oozie's
 * map-reduce action node for your type of map-reduce job, you can do the 
 * following:
 * <ul>
 *   <li>Write a standard map-reduce job that is executed from the 
 *   Java {@code main()} function (i.e. Oozie is not used here)</li>
 *   <li>Create a {@code Job} object and dump it to XML. This way you will
 *   know what are the default values of the job - these values don't have
 *   to be entered in Oozie action properties section.</li>
 *   <li>Configure the {@code Job} object according to the logic your map-reduce 
 *   job requires</li>
 *   <li>Create a {@code Job} object dump to XML. Now do the diff between
 *   the dump with default values and this one. By doing that you will know
 *   what properties are inserted/changed by your type of map-reduce job.</li>   
 * </ul>
 * In case of my code this looked like this:
 * <pre>{@code
 * 	@Override
 *	public int run(String[] args) throws Exception {
 * 		if (args.length != 2) {
 *			System.err.printf("Usage: %s [generic options] <input> <output>\n",
 *					getClass().getSimpleName());
 *			ToolRunner.printGenericCommandUsage(System.err);
 *			return -1;
 *		}
 *		
 *		
 *		Job job = new Job(getConf(), "Person cloner new API");
 *
 *		FileOutputStream outBefore = new FileOutputStream("before.xml"); 
 *		job.getConfiguration().writeXml(outBefore);
 *		outBefore.close();
 *
 *		FileInputFormat.addInputPath(job, new Path(args[0]));
 *		FileOutputFormat.setOutputPath(job, new Path(args[1]));
 *
 *		
 *		AvroJob.setInputKeySchema(job, Person.SCHEMA$);
 *		job.setInputFormatClass(AvroKeyInputFormat.class);
 *	
 *		job.setMapperClass(PersonClonerMapper.class);
 *		AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
 *		job.setOutputKeyClass(AvroKey.class);
 *		AvroJob.setMapOutputValueSchema(job, Person.SCHEMA$);
 *		job.setOutputValueClass(AvroValue.class);
 *		
 *		job.setReducerClass(PersonClonerReducer.class);
 *		AvroJob.setOutputKeySchema(job, User.SCHEMA$);
 *		job.setOutputFormatClass(AvroKeyOutputFormat.class);
 *
 *		FileOutputStream outAfter = new FileOutputStream("after.xml"); 
 *		job.getConfiguration().writeXml(outAfter);
 *		outBefore.close();
 *		
 *		job.waitForCompletion(true);
 *		return 0;
 *	}
 * }
 * }</pre>
 */
package eu.dnetlib.iis.core.javamapreduce;
