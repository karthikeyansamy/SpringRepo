package com.example.migrator;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;

public class SCPUploader {

    private static final Logger logger = LoggerFactory.getLogger(SCPUploader.class);

    public static void copyViaSCP(
            String localFile,
            String sshUser,
            String sshHost,
            int sshPort,
            String remotePath
    ) throws Exception {

        JSch jsch = new JSch();
        Session session = jsch.getSession(sshUser, sshHost, sshPort);

        // If key auth is needed:
        // jsch.addIdentity("/path/to/key");

        session.setPassword("your-ssh-password");
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect();

        ChannelSftp sftp = (ChannelSftp) session.openChannel("sftp");
        sftp.connect();

        logger.info("Uploading {} → {}", localFile, remotePath);
        sftp.put(new FileInputStream(localFile), remotePath);

        sftp.disconnect();
        session.disconnect();

        logger.info("SCP Upload complete.");
    }
}
