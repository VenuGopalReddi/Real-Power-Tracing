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

public class Cafs

{
      

public static class raghu extends Mapper<Object, Text, Text, IntWritable>

{

private final static IntWritable one = new IntWritable(1);

private static Text word = new Text();

static String k="";
 int e=0,pp,q,qq;
 
float a[][],rr[],gg[],bb[][],vr[][],mv[][];

float ir[][],ii[][],p[][],r,aa,cc,h,b,ee,d,f,m=0,n=0;

float v,u;

int x,y,z,l,g;
Text t=new Text();

public void map(Object a1,Text b1,Context c)throws IOException, InterruptedException

{

String s2=b1.toString();

String s1[]=s2.split(" ");

int s=Integer.parseInt(s1[0]);

int t=Integer.parseInt(s1[1]);

r=Float.parseFloat(s1[2]);

a=new float[t][s];

bb=new float[t][s];

rr=new float[t];

gg=new float[t];

vr=new float[t][s];

mv=new float[t][s];

ir=new float[t][s];

ii=new float[t][s];

p=new float[t][s];



e=3;

for(pp=0;pp<t;pp++)

{

for(q=0;q<s;q++)

{

a[pp][q]=Float.parseFloat(s1[e]);

e++;

}

}

for(int i=0;i<t;i++)

{

for(int j=0;j<s;j++)
{

rr[i]=rr[i]+a[i][j];

//r[i]+=temp;

}

//System.out.println(r[i]);

}

for(int i=0;i<t;i++)

{

for(int j=0;j<s;j++)

{

a[i][j]=a[i][j]/rr[i];

}

}

for(pp=0;pp<t;pp++)

{

for(qq=0;qq<s;qq++)

{


vr[pp][qq]=a[pp][qq]*r;

}

}

for(int i=0;i<t;i++)

{

for(int j=0;j<s;j++)

{
a[i][j]=1-a[i][j];

//System.out.println(a[i][j]);

}

}

for(int j=0;j<t;j++)

{

gg[j]=Float.parseFloat(s1[e]);

e++;

}

for(x=0;x<t;x++)

{

for(y=0;y<s;y++)

{


ir[x][y]=Float.parseFloat(s1[e]);


ii[x][y]=Float.parseFloat(s1[e]);

e++;

}

}


for(x=0;x<t;x++)

{

for(q=0;q<s;q++)

{


b=2;ee=2;d=0;f=0;




for(y=0;y<t;y++)

{

for(int tt=0;tt<s;tt++)


{




m=b*ir[y][tt];


b=m;

n=ee*ii[y][tt];


ee=n;




d=d+(ir[y][tt]*ir[y][tt]);




f=f+(ii[y][tt]*ii[y][tt]);


}

}


aa=(ir[x][q]*ir[x][q])+(ii[x][q]*ii[x][q]);


cc=(ir[x][q]*ir[x][q])/d;


h=(ii[x][q]*ii[x][q])/f;



v=b*cc;


u=ee*h;


p[x][q]=aa+v+u;


}

}

for(x=0;x<t;x++)

{

for(y=0;y<s;y++)

{

p[x][y]=p[x][y]*vr[x][y];


}

}

for(x=0;x<t;x++)

{

for(y=0;y<s;y++)

{

mv[x][y]=(a[x][y]*gg[x])/(s-1);

}

}



for(int i=0;i<t;i++)

{

for(int j=0;j<s;j++)

{

//System.out.println(g[i]);

//System.out.println(a[i][j]);

bb[i][j]=mv[i][j]+p[i][j];

k+=" "+bb[i][j]+" ";

word.set(k+" ");

//c.write(new Text(k),new IntWritable(1));

//System.out.println(b[i][j]);

}


}

c.write(word,one);


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
j.setJarByClass(Cafs.class);
j.setMapperClass(raghu.class);

//j.setCombinerClass("karthik.class");

//j.setReducerClass("karthik.class");

FileInputFormat.addInputPath(j,new Path(args[0]));

FileOutputFormat.setOutputPath(j,new Path(args[1]));

j.setOutputKeyClass(Text.class);

j.setOutputValueClass(IntWritable.class);

j.waitForCompletion(true);

}

}
