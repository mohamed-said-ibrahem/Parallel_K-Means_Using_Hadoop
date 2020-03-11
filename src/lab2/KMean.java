package lab2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMean {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		// Check for the I/P(s) , O/P(s).
		if(args.length != 4) {
			System.out.print("args should be 2 : <inputpath> <outpath> <number of centroids> <dimensions> .") ;
			System.exit(-1);
		}
		// Number of centroids required (clusters).
		int k = Integer.valueOf(args[2]) ;
		// Number of features for data point or centriod (dimentions)
		int dim = Integer.valueOf(args[3]) ;
		
		Job init_job = Job.getInstance() ;
		
		Configuration init_conf = init_job.getConfiguration();
		init_conf.set("kmeans.k", args[2]);
		init_conf.set("kmeans.dim", args[3]);
		
		init_job.setJarByClass(KMean.class);
		init_job.setJobName("clustered kmeans");
		init_job.setMapperClass(KMeanInitMapper.class);
		init_job.setReducerClass(KMeanInitReducer.class);
		init_job.setOutputKeyClass(IntWritable.class);
		init_job.setOutputValueClass(Text.class);
		
		
		FileInputFormat.addInputPath(init_job, new Path(args[0]));
		FileOutputFormat.setOutputPath(init_job, new Path(args[1] + "_m_" + Integer.toString(0)));
		
		/*The main reason for job.waitForCompletion exists is that its method call returns only when the job gets finished, 
		and it returns with its success or failure status which can be used to determine that further steps are to be run or not.
		Actually, the files are split into blocks and each block is executed on a separate node. 
		All the map tasks run in parallel and they are fed to the reducer after they are done. 
		There is no question of synchronization as you would think about in a multi-threaded program. In a multi-threaded program, 
		all the threads are running on the same box and since they share some of the data you have to synchronize them. 
		But here threads are not involved unless you use threads to submit jobs in parallel and wait for their completion. 
		For that, you have to use a job class instance per thread. */
		init_job.waitForCompletion(true);
		
		double[][] old_centroids = new double[k][dim] ;

		int fi = 1 ;
		
		// Used to calculate the time taken by operations(Parallel Hadoop K-Means)(MilliSeconds).
		long t1 =  System.currentTimeMillis() ;
		
		while(true) {
			
			System.out.print("start iteration") ;
			
			Job kmean_cluster_jb = Job.getInstance() ;
			Configuration conf = kmean_cluster_jb.getConfiguration() ;
			conf.set("kmeans.k", args[2]);
			conf.set("kmeans.dim", args[3]);
			
			kmean_cluster_jb.setJarByClass(KMean.class);
			kmean_cluster_jb.setJobName("clustered kmeans");
			kmean_cluster_jb.setMapperClass(KMeanIterationMapper.class);
			kmean_cluster_jb.setReducerClass(KMeanIterationReducer.class);
			kmean_cluster_jb.setOutputKeyClass(IntWritable.class);
			kmean_cluster_jb.setOutputValueClass(Text.class);
			
			
			// Configuration , I/P , O/P paths and buffer for operations.
			String uri =  args[1] + "_m_" + Integer.toString(fi-1) + "/part-r-00000";
			Configuration temp_conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), temp_conf); 
			Path input_path = new Path(uri);
			FSDataInputStream input_stream = fs.open(input_path);
			BufferedReader input_buffer = new BufferedReader(new InputStreamReader(input_stream));
	
			double total_dis = 0 ;
			
			double[][] new_centroids = new double[k][dim] ;
			for(int i = 0 ; i < k ; i++) {	
				String line = input_buffer.readLine() ;
				// New Centriods.
				if(line == null) {
					for(int j = 0 ; j < dim ; j++) {
						new_centroids[i][j] = Double.valueOf(old_centroids[i][j]) ;
					}
					continue ;
				}	
				int key = Integer.valueOf(line.split("\t")[0]) ;
				String[] new_centroid = line.split("\t")[1].split(",") ;
				for(int j = 0 ; j < dim ; j++) {
					new_centroids[key][j] = Double.valueOf(new_centroid[j]) ;
			// total distance btwn the new and old centriods (if no change we are done "no better solution than that").
					total_dis += Math.pow(new_centroids[key][j] - old_centroids[key][j], 2) ;
				}
				conf.set("kmeans.centroid" + key, line.split("\t")[1]);
			}
			
			double threshold = Math.pow(0.001 ,2) * k * dim  ;
			
			// Break the loop when the total change in centriods (new - old) is below (less than) a specific threshold.
			// Stop when there are no change in the centriods.
			if(total_dis < threshold)
				break ;
			
			FileInputFormat.addInputPath(kmean_cluster_jb, new Path(args[0]));
			FileOutputFormat.setOutputPath(kmean_cluster_jb, new Path(args[1] + "_m_" + Integer.toString(fi)));
			kmean_cluster_jb.waitForCompletion(true);
			old_centroids = new_centroids;
			fi++ ;
		}
		// To calculate the time between start and end of the job.
		long t2 =  System.currentTimeMillis() ;
		System.out.println("\n Time token by un-parallel is : " + (t2-t1) + "ms");
	}
	
}
