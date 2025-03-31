package com.temesoft.fs;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * Implementation for file storage service using SFTP (jsch)
 */
public class SftpFileStorageServiceImpl implements FileStorageService {

    final String remoteHost;
    final int remotePort;
    final String username;
    final String password;
    final String rootDirectory;
    final Properties configProperties;

    /**
     * Constructor taking SFTP details as arguments to set up SFTP
     */
    public SftpFileStorageServiceImpl(final String remoteHost,
                                      final int remotePort,
                                      final String username,
                                      final String password,
                                      final String rootDirectory,
                                      final Properties configProperties) {
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.username = username;
        this.password = password;
        this.rootDirectory = rootDirectory;
        this.configProperties = configProperties;
    }

    @Override
    public boolean exists(final FileStorageId<?> id) throws FileStorageException {
        try (final SftpSession sftpSession = new SftpSession()) {
            return exists(sftpSession, id);
        } catch (JSchException e) {
            throw new FileStorageException("Unable to verify file stats with ID: " + id, e);
        }
    }

    /**
     * Returns size of file content in bytes using provided id
     *
     * @param id - file id
     * @return - size of file in bytes
     * @throws FileStorageException - thrown when unable to get size of file
     */
    @Override
    public long getSize(final FileStorageId<?> id) throws FileStorageException {
        try (final SftpSession sftpSession = new SftpSession()) {
            if (!exists(sftpSession, id)) {
                throw new IOException("File does not exist");
            }
            final String fileName = getFileName(id.generatePath());
            final String folderPath = getParentPath(id.generatePath());
            sftpSession.getChannelSftp().cd(rootDirectory + FileStorageId.SEPARATOR + folderPath);
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            return sftpSession.getChannelSftp().stat(fileName).getSize();
        } catch (Exception e) {
            throw new FileStorageException("Unable to get file size with ID: " + id.value(), e);
        }
    }

    @Override
    public void create(final FileStorageId<?> id, final byte[] bytes) throws FileStorageException {
        try (final SftpSession sftpSession = new SftpSession()) {
            if (exists(sftpSession, id)) {
                throw new IOException("File already exist");
            }
            final String fileName = getFileName(id.generatePath());
            final String folderPath = getParentPath(id.generatePath());
            createDirectories(sftpSession, FileStorageId.SEPARATOR + folderPath);
            sftpSession.getChannelSftp().cd(rootDirectory + FileStorageId.SEPARATOR + folderPath);
            sftpSession.getChannelSftp().put(new ByteArrayInputStream(bytes), fileName);
        } catch (Exception e) {
            throw new FileStorageException("Unable to create file with ID: " + id.value(), e);
        }
    }

    @Override
    public void create(final FileStorageId<?> id, final InputStream inputStream, final long contentSize) throws FileStorageException {
        try (final SftpSession sftpSession = new SftpSession()) {
            if (exists(sftpSession, id)) {
                throw new IOException("File already exist");
            }
            final String fileName = getFileName(id.generatePath());
            final String folderPath = getParentPath(id.generatePath());
            createDirectories(sftpSession, FileStorageId.SEPARATOR + folderPath);
            sftpSession.getChannelSftp().cd(rootDirectory + FileStorageId.SEPARATOR + folderPath);
            sftpSession.getChannelSftp().put(inputStream, fileName);
        } catch (Exception e) {
            throw new FileStorageException("Unable to create file with ID: " + id.value(), e);
        }
    }

    @Override
    public void delete(final FileStorageId<?> id) throws FileStorageException {
        try (final SftpSession sftpSession = new SftpSession()) {
            if (!exists(sftpSession, id)) {
                throw new FileNotFoundException("File not found: " + id.generatePath());
            }
            final String fileName = getFileName(id.generatePath());
            final String folderPath = getParentPath(id.generatePath());
            sftpSession.getChannelSftp().cd(rootDirectory + FileStorageId.SEPARATOR + folderPath);
            sftpSession.getChannelSftp().rm(fileName);
        } catch (Exception e) {
            throw new FileStorageException("Unable to delete file with ID: " + id.value(), e);
        }
    }

    @Override
    public byte[] getBytes(final FileStorageId<?> id) throws FileStorageException {
        try (final SftpSession sftpSession = new SftpSession()) {
            final String fileName = getFileName(id.generatePath());
            final String folderPath = getParentPath(id.generatePath());
            sftpSession.getChannelSftp().cd(rootDirectory + FileStorageId.SEPARATOR + folderPath);
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            sftpSession.getChannelSftp().get(fileName, outputStream);
            return outputStream.toByteArray();
        } catch (SftpException | JSchException e) {
            throw new FileStorageException("Unable to get bytes from file with ID: " + id.value(), e);
        }
    }

    @Override
    public byte[] getBytes(final FileStorageId<?> id, final int startPosition, final int endPosition) throws FileStorageException {
        throw new FileStorageException("Method getBytes(...) by range is not implemented");
    }

    @Override
    public InputStream getInputStream(final FileStorageId<?> id) throws FileStorageException {
        try (final SftpSession sftpSession = new SftpSession()) {
            final String fileName = getFileName(id.generatePath());
            final String folderPath = getParentPath(id.generatePath());
            sftpSession.getChannelSftp().cd(rootDirectory + FileStorageId.SEPARATOR + folderPath);
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            sftpSession.getChannelSftp().get(fileName, outputStream);
            return new ByteArrayInputStream(outputStream.toByteArray());
        } catch (SftpException | JSchException e) {
            throw new FileStorageException("Unable to get input stream from file with ID: " + id.value(), e);
        }
    }

    private static final Collection<String> NOT_DELETABLE = Set.of(".", "..");

    @Override
    public void deleteAll() throws FileStorageException {
        try (final SftpSession sftpSession = new SftpSession()) {
            sftpSession.getChannelSftp().cd(rootDirectory);
            recursiveDelete(sftpSession.getChannelSftp(), rootDirectory);
        } catch (SftpException | JSchException e) {
            throw new FileStorageException("Unable to delete all available files", e);
        }
    }

    @Override
    public String getStorageDescription() {
        return "SFTP storage";
    }

    /**
     * Removes all directories and files within them recursively
     */
    private void recursiveDelete(final ChannelSftp sftp, final String path) throws SftpException {
        final Vector<ChannelSftp.LsEntry> files = sftp.ls(path);
        for (ChannelSftp.LsEntry file : files) {
            if (!NOT_DELETABLE.contains(file.getFilename())) {
                final String filePath = path + "/" + file.getFilename();
                if (file.getAttrs().isDir()) {
                    recursiveDelete(sftp, filePath);
                    sftp.rmdir(filePath);
                } else {
                    sftp.rm(filePath);
                }
            }
        }
    }

    /**
     * Helper class establishes SFTP connection session and auto-closes it once operation is complete
     */
    private class SftpSession implements Closeable {

        private final ChannelSftp channelSftp;

        public SftpSession() throws JSchException {
            final JSch jsch = new JSch();
            final Session session = jsch.getSession(username, remoteHost, remotePort);
            session.setPassword(password);
            if (configProperties != null) {
                session.setConfig(configProperties);
            }
            session.connect();
            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();
        }

        public ChannelSftp getChannelSftp() {
            return channelSftp;
        }

        @Override
        public void close() {
            channelSftp.disconnect();
        }
    }

    /**
     * Method verifies if file exists, taking established/open SFTP session
     */
    private boolean exists(final SftpSession sftpSession, final FileStorageId<?> id) throws FileStorageException {
        try {
            sftpSession.getChannelSftp().lstat(rootDirectory + FileStorageId.SEPARATOR + id.generatePath());
            return true; // File exists
        } catch (SftpException e) {
            if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                return false; // File does not exist
            }
            throw new FileStorageException("Unable to verify stats for file with ID: " + id, e);
        }
    }

    private String getParentPath(final String fullPath) {
        if (!fullPath.contains(FileStorageId.SEPARATOR)) {
            return fullPath;
        }
        return fullPath.substring(0, fullPath.lastIndexOf(FileStorageId.SEPARATOR));
    }

    private String getFileName(final String fullPath) {
        if (!fullPath.contains(FileStorageId.SEPARATOR)) {
            return fullPath;
        }
        return fullPath.substring(fullPath.lastIndexOf(FileStorageId.SEPARATOR) + 1);
    }

    private void createDirectories(final SftpSession sftpSession, final String path) throws SftpException {
        if (!path.contains(FileStorageId.SEPARATOR)) {
            return;
        }
        final String[] folders = path.split(FileStorageId.SEPARATOR);
        for (String folder : folders) {
            if (!folder.isEmpty()) {
                try {
                    sftpSession.getChannelSftp().cd(folder);
                } catch (SftpException e) {
                    sftpSession.getChannelSftp().mkdir(folder);
                    sftpSession.getChannelSftp().cd(folder);
                }
            }
        }
    }
}