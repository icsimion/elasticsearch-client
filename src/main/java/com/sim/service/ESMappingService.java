package com.sim.service;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by iliesimion.
 */
public class ESMappingService {
    private static final Logger LOG = Logger.getLogger(ESMappingService.class.getName());

    private String FILE_LOCATION = "src/main/resources/mappings/";

    public String readMappingFile(final String filename) {
        String fileToString = null;
        try {
            fileToString = FileUtils.readFileToString(new File(FILE_LOCATION + filename));
        } catch (IOException e) {
            LOG.log(Level.WARNING, e.getMessage());
        }

        return fileToString;
    }

}
