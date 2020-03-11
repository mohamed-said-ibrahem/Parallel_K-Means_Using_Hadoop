package lab2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeanIterationMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] dimensions = value.toString().split(",") ;
		Configuration conf = (Configuration) context.getConfiguration() ;
		int k = Integer.valueOf(conf.get("kmeans.k")) ;
		
		// Minimum  distance is used to assign each data point to the nearest centriod.
		double min_distance = Double.MAX_VALUE ;
		// best centroid index is the index for the nearest centroid classifier.
		int best_centroid_index = -1 ;
	
		// loop on the centriods number (2 cluster, 3 cluster ..etc).
		for ( int i = 0 ; i < k ; i ++) {
			
			String centroid = conf.get("kmeans.centroid" + Integer.toString(i)) ;
			// Split the centriods dimensions (features).
			String[] centroid_dim_split = centroid.split(",") ;
			
			// distance between each data point and a specific centriod.
			double distance = 0 ;
			// loop on the dimensions (which is the colms of the single row) EX. a,b,c,d --> 4 dimensions.  
			for(int j = 0 ; j < dimensions.length-1  ; j++) {
				double point_dim = Double.parseDouble(dimensions[j]) ;
				double centroid_dim = Double.parseDouble(centroid_dim_split[j]) ;
				// calculate the distance between centriod and data point.
				distance += Math.pow(point_dim - centroid_dim, 2) ; 
			}
			
			if(distance < min_distance) {
				min_distance = distance ; 
				best_centroid_index = i ;
			}
		}
		// Select the best centriod for the specific data point.
		context.write(new IntWritable(best_centroid_index) , value);
	}
	
}
