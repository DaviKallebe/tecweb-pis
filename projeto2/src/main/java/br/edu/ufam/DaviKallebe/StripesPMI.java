package br.edu.ufam.DaviKallebe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


public class StripesPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(StripesPMI.class);

    //contanto mapear (A, 1) e (B, 1) e (A_B, 1)
    public static final class CounterMapperWord extends Mapper<LongWritable, Text, Text, IntWritable> {

        //constantes
        private static final Pattern PATTERN = Pattern.compile("(^[^a-z]+|[^a-z]+$)");
        private static final String EMPTY_STRING = "";
        //reuso
        private static final IntWritable VALUE = new IntWritable();
        private static final Text WORD = new Text();        

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String lines[] = value.toString().split("\\r?\\n");
            Map<String, Integer> unique = new HashMap<String, Integer>();
            int count = 0;

            for (String line: lines) {
                StringTokenizer tk = new StringTokenizer(line);

                while (tk.hasMoreTokens()) {
                    String word = PATTERN.matcher(tk.nextToken().toLowerCase()).replaceAll("");

                    if (word.length() != 0){
                        if (unique.containsKey(word))
                            unique.put(word, unique.get(word) + 1);
                        else
                            unique.put(word, 1);

                        count = count + 1;
                    }
                }
            }

            unique.put("totalOfEntryInThisFileIs", count);

            for (Map.Entry<String, Integer> entry : unique.entrySet()) {
                WORD.set(entry.getKey());
                VALUE.set(entry.getValue());

                context.write(WORD, VALUE);
            }
        }
    }

    private static class CounterReduceWord extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final static IntWritable COUNT = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;

            for (IntWritable value: values)
                count = count + value.get();

            COUNT.set(count);
            context.write(key, COUNT);
        }
    }

    public static final class CounterMapperPair extends Mapper<LongWritable, Text, Text, Text> {

        //constantes
        private static final Pattern PATTERN = Pattern.compile("(^[^a-z]+|[^a-z]+$)");
        private static final String EMPTY_STRING = "";
        //reuso
        private static final Text KEY = new Text();         
        private static final Text VALUE = new Text();      

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Map<String, Map<String, Integer>> mpairs = new HashMap<String, Map<String, Integer>>();

            String lines[] = value.toString().split("\\r?\\n");

            for (String line: lines) {
                StringTokenizer tk = new StringTokenizer(line);
                String left = EMPTY_STRING;

                while (tk.hasMoreTokens()) {
                    String right = PATTERN.matcher(tk.nextToken().toLowerCase()).replaceAll("");

                    if (right.length() > 0 && left.length() > 0) {
                        if (mpairs.containsKey(left)) {
                            if (mpairs.get(left).containsKey(right))
                                mpairs.get(left).put(right, mpairs.get(left).get(right));
                            else
                                mpairs.get(left).put(right, 1);
                        }
                        else {
                            Map<String, Integer> subpairs = new HashMap<String, Integer>();
                            subpairs.put(right, 1);
                            mpairs.put(left, subpairs);
                        }
                    }

                    left = right;
                }
            }
            
            for (Map.Entry<String, Map<String, Integer>> entry : mpairs.entrySet()) {
                String stripKey = "";
                Boolean first = true;
                
                for (Map.Entry<String, Integer> subentry : entry.getValue().entrySet()) {
                    if (first)
                        stripKey = stripKey + subentry.getKey() + ":" + Integer.toString(subentry.getValue());
                    else
                        stripKey = stripKey + "," + subentry.getKey() + ":" + Integer.toString(subentry.getValue());                    
                }
                
                KEY.set(entry.getKey());
                VALUE.set(stripKey);

                context.write(KEY, VALUE);
            }
        }
    }

    private static class CounterReducePair extends Reducer<Text, Text, Text, Text> {
        private final static IntWritable COUNT = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //
        }
    }

    //calcular o PMI
    private static class PMIReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private static Map<String, Integer> words;        
        private static DoubleWritable DPMI = new DoubleWritable();

        @Override
        public void setup(Context context)
                throws IOException {
            words = new HashMap<String, Integer>();

            Configuration conf = context.getConfiguration();
            String firstOutput = conf.get("firstOutput");
            FileSystem fs = FileSystem.get(conf);
            Path file_dir = new Path(firstOutput);
            RemoteIterator<LocatedFileStatus> fileListItr = fs.listFiles(file_dir, true);

            while (fileListItr .hasNext()){
                LocatedFileStatus file = fileListItr.next();

                if (file.getPath().getName().contains("part-r-")) {
                    FSDataInputStream fdis = fs.open(file.getPath());
                    InputStreamReader inStream = new InputStreamReader(fdis);
                    BufferedReader br = new BufferedReader(inStream);

                    try {
                        for (String line = br.readLine(); line != null; line = br.readLine()) {
                            String[] columns = line.split("\\s+");

                            if (columns.length == 2)
                                words.put(columns[0], Integer.parseInt(columns[1]));
                        }
                    } catch(FileNotFoundException e) {
                        throw new IOException("Arquivo de saída do estágio 1 não encontrado");
                    } finally {
                        br.close();
                    }
                }
            }                        
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //
        }
    }

    private StripesPMI() {}

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        String output;

        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        int numReducers = 1;
    }

    @Override
    public int run(String[] argv) throws Exception {
        final Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));
        String firstOutput = "word_count";

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

        LOG.info("Tool: " + StripesPMI.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - number of reducers: " + args.numReducers);
        LOG.info("Começando JOB 1");

        Configuration conf1 = getConf();
        Job job1 = Job.getInstance(conf1);
        job1.setJobName(StripesPMI.class.getSimpleName() + ": Estágio 1 - Contando Palavras");
        job1.setJarByClass(StripesPMI.class);

        job1.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job1, new Path(args.input));
        FileOutputFormat.setOutputPath(job1, new Path(firstOutput));

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setMapperClass(CounterMapperWord.class);
        job1.setCombinerClass(CounterReduceWord.class);
        job1.setReducerClass(CounterReduceWord.class);

        Path outputDir1 = new Path(firstOutput);
        FileSystem.get(conf1).delete(outputDir1, true);

        long startTime = System.currentTimeMillis();
        job1.waitForCompletion(true);

        LOG.info("Começando JOB 2");

        Configuration conf2 = getConf();
        conf2.set("firstOutput", firstOutput);

        Job job2 = Job.getInstance(conf2);
        job2.setJobName(StripesPMI.class.getSimpleName() + ": Estágio 2 - Contando Pares & Calculando PMI");
        job2.setJarByClass(StripesPMI.class);

        job2.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job2, new Path(args.input));
        FileOutputFormat.setOutputPath(job2, new Path(args.output));

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setMapperClass(CounterMapperPair.class);
        job2.setCombinerClass(CounterReducePair.class);
        job2.setReducerClass(PMIReducer.class);

        Path outputDir2 = new Path(args.output);
        FileSystem.get(conf2).delete(outputDir2, true);

        job2.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        LOG.info("Job terminado em " + (System.currentTimeMillis() - startTime) / 1000.0 + " segundos");

        //Path outputDir = new Path(args.output);
        //FileSystem.get(conf).delete(outputDir, true);

        return 0;
    }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}

