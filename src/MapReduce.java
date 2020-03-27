import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.*;

public class MapReduce {
    private static Scanner x;

    /**
     * File[1] File[2] numOfThreadsInPool
     * time the Mapping, Grouping and Reducing phases of the application
     * use thread pool
     * compare thread pool sizes
     * compare file sizes
     * compare number of files
     */

    /**
     * @param args
     * args[0] int: number of threads
     * args[1:x] String: file locations, x = number of files
     * x = number of arguments-2 ... (first argument is a int and x is an index)
     */

    /////////////////TESTING//////////////////////////////////////////////////////////////////////////

    static LinkedList<File> files = new LinkedList<>();
    static File test1 = new File("src/file1.txt");
    static File test2 = new File("src/file2.txt");

    ///////////////////////////////////////////////////////////////////////////////////////////////////

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        files.add(test1);
        files.add(test2);
        if(test1.exists() && test2.exists()){
            System.out.println("test1 exist");
        }
        Map<String, String> input = new HashMap<String, String>();
        int numberOfThreads = 2;
        try {
            numberOfThreads= Integer.parseInt(args[0]);
            for (int i = 1; i < args.length; i++) {
                files.add(new File(args[i]));
            }
        }catch(ArrayIndexOutOfBoundsException e){
            //System.out.println("No input arguments");
            //System.exit(0);
            /**
             * commented out for testing
             */
        }
        catch(NumberFormatException e){
            System.out.println("First argument must be a number");
            System.exit(0);
        }

        for (File file:files) {
            try{
                x = new Scanner(file);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                System.out.println("Cannot find txt file");
            }
            String w = "";

            while(x.hasNext()){
                String a = x.next();
                w = w + " " + a;

            }
            input.put(file.getName(),w);
            x.close();
        }
        input.put("file3.txt", "foo foo foo bird");

        ExecutorService pool = Executors.newFixedThreadPool(numberOfThreads);
        ArrayList<Future> futures = new ArrayList<>();

        {
            Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            // MAP:

            List<MappedItem> mappedItems = new LinkedList<MappedItem>();

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                String file = entry.getKey();
                String contents = entry.getValue();
                Future f = pool.submit(() -> {
                    map(file, contents, mappedItems);
                });
                futures.add(f);
            }
            for(Future f: futures){
                f.get();
            }
            futures.clear();
            // GROUP:

            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }

            // REDUCE:

            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while(groupedIter.hasNext()) {
                Map.Entry<String, List<String>> entry = groupedIter.next();
                String word = entry.getKey();
                List<String> list = entry.getValue();
                Future f = pool.submit(()-> reduce(word, list, output));
                futures.add(f);
            }
        for(Future<?> f: futures){ // waits for all threads to finish
            f.get(); // blocks future till all thread are finished
        }
            //  System.out.println(output);
        futures.clear(); // for the next test
        }

        // APPROACH #3: Distributed MapReduce
        {
            final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            // MAP:

            final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

            final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                @Override
                public synchronized void mapDone(String file, List<MappedItem> results) {
                    mappedItems.addAll(results);
                }
            };

            List<Thread> mapCluster = new ArrayList<Thread>(input.size());

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                final String file = entry.getKey();
                final String contents = entry.getValue();
                //removed creating thread, uses thread from pool
                Future f = pool.submit(() -> map(file, contents, mapCallback));
                futures.add(f);
            }
            for(Future<?> f: futures){
                f.get();
            }
            // GROUP:
            futures.clear();
            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }

            // REDUCE:

            final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                @Override
                public synchronized void reduceDone(String k, Map<String, Integer> v) {
                    output.put(k, v);
                }
            };

            List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());

            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while(groupedIter.hasNext()) {
                Map.Entry<String, List<String>> entry = groupedIter.next();
                final String word = entry.getKey();
                final List<String> list = entry.getValue();
                //removed creating thead, using thread from thread pool
                Future f = pool.submit(() -> reduce(word, list, reduceCallback));
                futures.add(f);
            }

            for(Future<?> f: futures){ // waits for all threads to finish
                f.get(); // blocks future till all thread are finished
            }
              System.out.println(output);
            futures.clear(); // for the next test
        }
    System.exit(0);
    }


    public static void map(String file, String contents, List<MappedItem> mappedItems) {
        String[] words = contents.trim().split("\\s+");
        for(String word: words) {
            mappedItems.add(new MappedItem(word, file));
        }
    }

    public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        output.put(word, reducedList);
    }

    public static interface MapCallback<E, V> {

        public void mapDone(E key, List<V> values);
    }

    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<MappedItem>(words.length);
        for(String word: words) {
            results.add(new MappedItem(word, file));
        }
        callback.mapDone(file, results);
    }

    public static interface ReduceCallback<E, K, V> {

        public void reduceDone(E e, Map<K,V> results);
    }

    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        callback.reduceDone(word, reducedList);

    }

    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }

}

