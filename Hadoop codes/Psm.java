import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class Psm
{
      
public static class ram extends Mapper<Object, Text, Text, IntWritable>
{
private final static IntWritable one = new IntWritable(1);
private static Text word = new Text();
static String k="";
 float g[],l[],h,b[][];
int gb,lb,i,j,e=0;
Text t=new Text();
public void map(Object a1,Text b1,Context c)throws IOException, InterruptedException
{
String s2=b1.toString();
String s1[]=s2.split(" ");
gb=Integer.parseInt(s1[0]);
lb=Integer.parseInt(s1[1]);
g=new float[gb];
l=new float[lb];
b=new float[lb][gb];
e=2;
for(i=0;i<gb;i++)
{
g[i]=Float.parseFloat(s1[e]);
e++;
}
for(i=0;i<lb;i++)
{
l[i]=Float.parseFloat(s1[e]);
e++;
}

for(j=0;j<gb;j++)
{
h+=g[j];
}
for(i=0;i<lb;i++)
{
for(j=0;j<gb;j++)
{
b[i][j]=l[i]*(g[j]/h);
//k+=" "+b[i][j]+" ";
//word.set(k+" "+s1.length);
//c.write(word,one);
}
}





/*for(int i=e,j=0;i<s1.length;i++,j++)
{
g[j]=Float.parseFloat(s1[i]);
}*/
for(int i=0;i<lb;i++)
{
for(int j=0;j<gb;j++)
{
//System.out.println(g[i]);
//System.out.println(a[i][j]);
//b[i][j]=a[i][j]*g[i];
k+=" "+b[i][j]+" ";
word.set(k+" "+s1.length);
//c.write(new Text(k),new IntWritable(1));
//System.out.println(b[i][j]);
}
}c.write(word,one);
}
}




/*private class karthik extends reducer<Text,Intwritable,Text,Intwritable>
{
int sum;
IntWritable n=new IntWritable();
public void reduce(Text x,IntWritable<iterable> y,Context z)
{
for(IntWritable v1:y)
{
sum+=v1.get();
}
z.write(x,n.set(sum))	
}
}*/
public static void main (String[] args)throws Exception
{
Configuration c=new Configuration();
c.addResource(new Path("/home/hadoop/Desktop/hadoop/conf/core-site.xml"));
c.addResource(new Path("/home/hadoop/Desktop/hadoop/conf/hdfs-site.xml"));
c.addResource(new Path("/home/hadoop/Desktop/hadoop/conf/mapred-site.xml"));
Job j=new Job(c,"snarc");
j.setJarByClass(Psm.class);
j.setMapperClass(ram.class);
//j.setCombinerClass("karthik.class");
//j.setReducerClass("karthik.class");
FileInputFormat.addInputPath(j,new Path(args[0]));
FileOutputFormat.setOutputPath(j,new Path(args[1]));
j.setOutputKeyClass(Text.class);
j.setOutputValueClass(IntWritable.class);
j.waitForCompletion(true);
}
}

