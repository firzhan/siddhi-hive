/*
 *
 *  *
 *  *  * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *  *  *
 *  *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  *  * you may not use this file except in compliance with the License.
 *  *  * You may obtain a copy of the License at
 *  *  *
 *  *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 */

package org.wso2.carbon.siddhihive.core.utils;


import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 *  Utility tool to zip as BAM tool box
 */
public class ZipppingUtil {

	private static final Logger log = Logger.getLogger(ZipppingUtil.class);

    List<String> fileList;
	private File sourceFolder;

    public ZipppingUtil() {
        fileList = new ArrayList<String>();
    }

    public void zip(String zipFile, File sourceFile) {
        sourceFolder = sourceFile;
	    generateFileList(sourceFile);
        byte[] buffer = new byte[1024];

        try {

            FileOutputStream fos = new FileOutputStream(zipFile);
            ZipOutputStream zos = new ZipOutputStream(fos);

            System.out.println("Output to Zip : " + zipFile);

            for (String file : this.fileList) {

                ZipEntry ze = new ZipEntry(file);
                zos.putNextEntry(ze);

                FileInputStream in =
                        new FileInputStream(sourceFile + File.separator + file);

                int len;
                while ((len = in.read(buffer)) > 0) {
                    zos.write(buffer, 0, len);
                }
                in.close();
            }
            zos.closeEntry();
            zos.close();
            System.out.println("Done");
        } catch (IOException ex) {
            ex.printStackTrace();
	        log.error("Toolbox zipping operation failed", ex);
        }
    }

    public void generateFileList(File node) {

        if (node.isFile()) {
            fileList.add(generateZipEntry(node.getAbsoluteFile().toString()));
        }
        if (node.isDirectory()) {
            String[] subNote = node.list();
            for (String filename : subNote) {
                generateFileList(new File(node, filename));
            }
        }

    }

    private String generateZipEntry(String file) {
        return file.substring(sourceFolder.getAbsolutePath().length() + 1, file.length());
    }

}
