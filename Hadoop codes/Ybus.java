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
public class Ybus
{
      
public static class venu extends Mapper<Object, Text, Text, IntWritable>
{
private final static IntWritable one = new IntWritable(1);
private static Text word = new Text();
static String k="";
int l,g,a,j,e=0;
float f[][],v[][],vg[],p[][],i[];
Text t=new Text();
public void map(Object a1,Text b1,Context c)throws IOException, InterruptedException
{
String s2=b1.toString();
String s1[]=s2.split(" ");
int l=Integer.parseInt(s1[0]);
int g=Integer.parseInt(s1[1]);
f=new float[l][g];
v=new float[l][g];
vg=new float[g];
p=new float[l][g];
i=new float[l];

e=2;
for(a=0;a<l;a++)
{
for(j=0;j<g;j++)
{
f[a][j]=Float.parseFloat(s1[e]);
e++;
}
}
for(a=0;a<g;a++)
{
vg[a]=Float.parseFloat(s1[e]);
e++;
}
for(j=0;j<l;j++)
{
i[j]=Float.parseFloat(s1[e]);
e++;
}
for(a=0;a<l;a++)
{
for(j=0;j<g;j++)
{
v[a][j]=f[a][j]*vg[j];

}
//System.out.println(v[a]);
}
for(a=0;a<l;a++)
{
for(j=0;j<g;j++)
{
p[a][j]=v[a][j]*i[a];
}
}

for(int a=0;a<l;a++)
{
for(int j=0;j<g;j++)
{
k+=""+p[a][j]+"	";
word.set(k);

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
j.setJarByClass(Ybus.class);
j.setMapperClass(venu.class);
//j.setCombinerClass("karthik.class");
//j.setReducerClass("karthik.class");
FileInputFormat.addInputPath(j,new Path(args[0]));
FileOutputFormat.setOutputPath(j,new Path(args[1]));
j.setOutputKeyClass(Text.class);
j.setOutputValueClass(IntWritable.class);
j.waitForCompletion(true);
}
}


