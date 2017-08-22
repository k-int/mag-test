package com.k_int.imu.test;

import com.kesoftware.imu.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;

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

    private static final File errorsFile = new File("Errors.txt");
    private static final File processedFile = new File("processedIRN.txt");

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
        	FileUtils.writeStringToFile(errorsFile, "", StandardCharsets.UTF_8, false);
        	FileUtils.writeStringToFile(processedFile, "", StandardCharsets.UTF_8, false);

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
                try {
                    ModuleFetchResult results;
                    if (count == 0L)
                        results = module.fetch("start", 0, resultSetSize, fetchFields());
                    else
                        results = module.fetch("current", 0, resultSetSize, fetchFields());
                    // gets the results from the result object returned
                    Map[] rows = results.getRows();
                    iterateResultSet(rows);
                    count = count + resultSetSize;
                }
                catch(Exception e){
                    FileUtils.write(errorsFile, "Error obtaining the result set : " + e.toString() + "\n", StandardCharsets.UTF_8, true);

                }
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
                String resource = null;
            try{
                String publish = row.getString("AdmPublishWebNoPassword");
                String multimedia = row.getString("Multimedia");
                Path path = Paths.get(multimedia);
                FileUtils.write(processedFile, irn + " : " + multimedia + "\n", StandardCharsets.UTF_8, true);
                processResource(row.getMap("resource"), irn, path);
            }
            catch(Exception e){
                try {
                    if (row.get("resource") == null)
                        FileUtils.write(errorsFile, irn + " : error creating media, resource is null " + e.toString() + "\n", StandardCharsets.UTF_8, true);
                    else
                        FileUtils.write(errorsFile, irn + " : error creating media " + e.toString() + "\n", StandardCharsets.UTF_8, true);
                    }
                catch (Exception ee){
                    ee.printStackTrace();
                    throw ee;
                }
                e.printStackTrace();
            }
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
                f =  new File(rel + identifier);
                System.out.println(rel+identifier);
                // Note: this closes the source as well
                FileUtils.copyInputStreamToFile(temp, f);
            } catch (Exception e) {
                try {
                    FileUtils.write(errorsFile, irn + " : error creating media " + e.getMessage() + "\n", StandardCharsets.UTF_8, true);
                    e.printStackTrace();
                } catch(Exception ee){
                    ee.printStackTrace();
                }
            }
        } else {
            System.out.println("resource is null");
            FileUtils.write(errorsFile, irn + " : resource was null " + "\n", StandardCharsets.UTF_8, true);
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
