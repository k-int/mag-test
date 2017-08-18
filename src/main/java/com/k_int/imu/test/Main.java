package com.k_int.imu.test;

import com.kesoftware.imu.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Created by michael Sagar - KINT on 16/08/2017.
 * Created to aid the investigation of obtaining
 * the images from the Emu database at Manchester Art Galleries
 */
public class Main {
     // ip address to connect to
    static String ip = null;
    // port to connect to
    static Integer port = null;
    // result set size
    final static int resultSetSize = 10;


    public static void main(String[] args) {

        if(args.length>1) {
         if(args[0] != null)
             ip = args[0];
         if(args[1] != null)
             port = Integer.valueOf(args[1]);
        }

        // this shows the version of Imu jar file that is being used
        System.out.println("IMu version "+ IMu.VERSION);

        //creating empty session to be used later
        Session session = null;
        try {
            //creating logfiles
            File f = new File("Errors.txt");
            if(!f.exists())
                new FileOutputStream(f).close();
            f = new File("processedIRN.txt");
            if(!f.exists())
                new FileOutputStream(f).close();

            // initialising the session
            session = new Session(ip, port);
            // initialising the module
            Module module = createModule("emultimedia",session);

            // Used to filter out unneeded media. These are media records which are not linked to
            Long hits = filerUnlinkedMedia(module);

            /**
             * the loop is uses to iterate over the rusults getting back the correct resultSet each time
             */
            Long count = 0L;
            while(count <= hits){
                 /*
             * This gets the results paginated 0-1000. Using "start" states that this is the start of the
             * results, "current" means use the current results but get the next n abount of records
             */
                ModuleFetchResult results;
                if(count==0L)
                    results = module.fetch("start", 0, resultSetSize, fetchFields());
                else
                    results = module.fetch("current", 0, resultSetSize, fetchFields());
                // gets the results from the result object returned
                Map[] rows = results.getRows();
                iterateResultSet(rows);
                count = count + resultSetSize;
            }

        }
        catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        finally{
            if(session != null)
                session.disconnect();
            System.out.println("FINISHED");
        }

    }

    private static Long filerUnlinkedMedia(Module module)throws Exception{
        Terms search = new Terms(TermsKind.OR);
//        search.add("objects=<ecatalogue:MulMultiMediaRef_tab>.(irn)","*");
//        search.add("events=<eevents:MulMultiMediaRef_tab>.(irn)","*");
//        search.add("agents=<eparties:MulMultiMediaRef_tab>.(irn)","*");
//        search.add("narratives=<enarratives:MulMultiMediaRef_tab>.(irn)","*");
        Long hits = module.findTerms(search);
        System.out.println("hits: "+hits);
        return hits;
    }

    /**
     * Creates module for connecting and searching Emu
     * @param module   - the module to connect to in emu (ecatalogue, emultimedia, etc)
     * @param session  - the session to use for the connection
     * @return  - returns the newly created module
     * @throws Exception
     */
    private static Module createModule(String module,Session session) throws Exception {
        return new Module(module, session);
    }

    /**
     * iterate throught the resultSet returned from Emu
     * @param resultSet - is the results given from Imu as a KE Map array
     */
    private static void iterateResultSet(Map[] resultSet) throws Exception{
        for(Map row : resultSet){
            String irn = row.getString("irn");
            String publish = row.getString("AdmPublishWebNoPassword");
            String multimedia = row.getString("Multimedia");
            Path path = Paths.get(multimedia);
            Files.write(Paths.get("processedIRN.txt"), (irn + " : "+multimedia+"\n").getBytes(), StandardOpenOption.APPEND);
            processResource(row.getMap("resource"), irn, path);
        }
    }

    private static void processResource(Map resource, String irn, Path path)throws Exception{
        if (resource != null) {
            String identifier = resource.getString("identifier");
            String mimeType = resource.getString("mimeType");
            String mimeFormat = resource.getString("mimeFormat");
            String date = resource.getString("AdmDateModified");
            String height = resource.getString("height");
            String width = resource.getString("width");
            String source = resource.getString("source");
            long size = resource.getLong("size");
            System.out.format("identifier: %s%n", identifier);
            System.out.format("date: %s%n", date);
            System.out.format("mimeType: %s%n", mimeType);
            System.out.format("mimeFormat: %s%n", mimeFormat);
            System.out.format("size: %d%n", size);
            System.out.format("height: %s%n", height);
            System.out.format("width: %s%n", width);
            System.out.format("source: %s%n", source);
//
            try {
                //getting the actual image from the record
                FileInputStream temp = (FileInputStream) resource.get("file");
                // setting up the path to save the file in
                String paths = "images/"+path.toString();
                File full = new File(paths);
                String rel = full.getCanonicalPath().replace(full.getName(),"");
                File f =  new File(rel);
                f.mkdirs();
                System.out.println(rel+identifier);
                FileOutputStream copy = new FileOutputStream(rel+identifier);
                byte[] buffer = new byte[16096];
                while (temp.read(buffer) > 0)
                    copy.write(buffer);
                 copy.close();
            } catch (Exception e) {
                try {
                    Files.write(Paths.get("Errors.txt"), (irn + " : error creating media " + e.getMessage() + "\n").getBytes(), StandardOpenOption.APPEND);
                    e.printStackTrace();
                } catch(Exception ee){
                    ee.printStackTrace();
                }
            }
        } else {
            System.out.println("resource is null");
            Files.write(Paths.get("Errors.txt"), (irn + " : resource was null " + "\n").getBytes(), StandardOpenOption.APPEND);


        }
    }

    /**
     * This defines the requested fields from Emu
     * @return - a string array containing the requested fields
     */
    private static String[] fetchFields(){
        return new String[]
                {
                        "irn",     // the identifier value
                        "AdmPublishWebNoPassword",     // this is used to determain if the record is public
                        "Multimedia",    // the original multimedia value

                        /*
                         Below is the main part which is up for investigation,
                         resource contains the detail of the image including the image
                         Only use one of the below
                          */
                        //"resource(kind==resolution;width@1200;format==jpeg)" // resource is null when using this - this is what was supplied, does not confom to the documentation
                        "resource{kind:resolution,width:1200,format:jpeg}" //this is what I belive it should be

                };
    }


}
