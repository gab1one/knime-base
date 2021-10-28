/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * Created on Aug 22, 2013 by wiswedel
 */
package org.knime.base.node.util.sendmail;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

import javax.activation.FileTypeMap;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.internet.MimeUtility;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.knime.base.util.flowvariable.FlowVariableProvider;
import org.knime.base.util.flowvariable.FlowVariableResolver;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.util.StringHistory;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.util.FileUtil;

/**
 * Configuration proxy for the node.
 *
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 */
final class SendMailConfiguration {

    /**
     * A system property that, if set, will disallow emails sent to recipients other than specified in a comma separate
     * list. For instance-D{@value #PROPERTY_ALLOWED_RECIPIENT_DOMAINS}=foo.com,bar.org would allow only emails to be
     * sent to foo.com and bar.org. If other recipients are specified the node will fail during execution. If this
     * property is not specified or empty all domains are allowed.
     */
    public static final String PROPERTY_ALLOWED_RECIPIENT_DOMAINS = "knime.sendmail.allowed_domains";

    private static final int DEFAULT_SMTP_CONNECTION_TIMEOUT = 2000;

    private static final int DEFAULT_SMTP_READ_TIMEOUT = 30000;

    /** EMail format. */
    enum EMailFormat {
            /** Ordinary text. */
            Text,
            /** Text w/ html tags. */
            Html;
    }

    /** Connection Security. */
    enum ConnectionSecurity {
            /** No security. */
            NONE,
            /** StartTLS. */
            STARTTLS,
            /** Via ssl. */
            SSL
    }

    /** EMail priority. */
    enum EMailPriority {
            /** Prio 1. */
            Highest(1),
            /** Prio 2. */
            High(2),
            /** Prio 3. */
            Normal(3),
            /** Prio 4. */
            Low(4),
            /** Prio 5. */
            Lowest(5);

        private final int m_priority;

        private EMailPriority(final int priority) {
            m_priority = priority;
        }

        /** @return for instance "1 (Highest)". */
        String toXPriority() {
            return m_priority + " (" + name() + ")";
        }
    }

    private String m_smtpHost;

    private int m_smtpPort;

    private boolean m_useAuthentication;

    private boolean m_useCredentials;

    private String m_credentialsId;

    private String m_smtpUser;

    private String m_smtpPassword;

    private ConnectionSecurity m_connectionSecurity = ConnectionSecurity.NONE;

    private EMailFormat m_format = EMailFormat.Text;

    private EMailPriority m_priority = EMailPriority.Normal;

    private String m_from;

    private String m_to;

    private String m_cc;

    private String m_bcc;

    private String m_replyTo;

    private String m_subject;

    private String m_text;

    private URL[] m_attachedURLs = new URL[0];

    private int m_smtpConnectionTimeout = DEFAULT_SMTP_CONNECTION_TIMEOUT;

    private int m_smtpReadTimeout = DEFAULT_SMTP_READ_TIMEOUT;

    /**
     * Save to argument settings.
     *
     * @param settings ...
     */
    void saveConfiguration(final NodeSettingsWO settings) {
        if (m_smtpHost != null) {
            settings.addString("smtpHost", m_smtpHost);
            settings.addInt("smtpPort", m_smtpPort);
            settings.addBoolean("useAuthentication", m_useAuthentication);
            settings.addBoolean("useCredentials", m_useCredentials);
            settings.addString("credentialsId", m_credentialsId);
            settings.addString("smtpUser", m_smtpUser);
            if (!m_useCredentials) { // only save password as needed, see AP-15442
                settings.addPassword("smtpPasswordWeaklyEncrypted", "0=d#Fs64h", m_smtpPassword);
            }
            settings.addString("connectionSecurity", m_connectionSecurity.name());
            settings.addString("emailFormat", m_format.name());
            settings.addString("emailPriority", m_priority.name());
            settings.addString("from", m_from);
            settings.addString("to", m_to);
            settings.addString("cc", m_cc);
            settings.addString("bcc", m_bcc);
            settings.addString("replyTo", m_replyTo);
            settings.addString("subject", m_subject);
            settings.addString("text", m_text);
            var urlsAsArray = new String[m_attachedURLs.length];
            for (var i = 0; i < urlsAsArray.length; i++) {
                urlsAsArray[i] = m_attachedURLs[i].toString();
            }
            settings.addStringArray("attachedURLs", urlsAsArray);

            settings.addInt("smtpConnectionTimeout", m_smtpConnectionTimeout);
            settings.addInt("smtpReadTimeout", m_smtpReadTimeout);
        }
    }

    /**
     * Loader in dialog (with defaults).
     *
     * @param settings ...
     */
    void loadConfigurationInDialog(final NodeSettingsRO settings) {
        int defaultPort;
        try {
            var lastPortString = getLastUsedHistoryElement(getSmtpPortStringHistoryID());
            defaultPort = Integer.parseInt(lastPortString);
        } catch (Exception e) { // NOSONAR backwards compatible loading
            defaultPort = 25;
        }
        m_smtpHost = settings.getString("smtpHost", getLastUsedHistoryElement(getSmtpHostStringHistoryID()));
        m_smtpPort = settings.getInt("smtpPort", defaultPort);
        m_useAuthentication = settings.getBoolean("useAuthentication", false);
        m_useCredentials = settings.getBoolean("useCredentials", false);
        m_credentialsId = settings.getString("credentialsId", "");
        m_smtpUser = settings.getString("smtpUser", getLastUsedHistoryElement(getSmtpUserStringHistoryID()));
        if (settings.containsKey("smtpPassword")) { // until v2.11 (excl)
            m_smtpPassword = settings.getString("smtpPassword", "");
        } else {
            m_smtpPassword = settings.getPassword("smtpPasswordWeaklyEncrypted", "0=d#Fs64h", "");
        }
        var connectionSecurityS = settings.getString("connectionSecurity", ConnectionSecurity.NONE.name());
        ConnectionSecurity connectionSecurity;
        try {
            connectionSecurity = ConnectionSecurity.valueOf(connectionSecurityS);
        } catch (Exception e) { // NOSONAR backwards compatible loading
            connectionSecurity = ConnectionSecurity.NONE;
        }
        m_connectionSecurity = connectionSecurity;
        var formatS = settings.getString("emailFormat", EMailFormat.Text.name());
        EMailFormat format;
        try {
            format = EMailFormat.valueOf(formatS);
        } catch (Exception e) { // NOSONAR backwards compatible loading
            format = EMailFormat.Text;
        }
        m_format = format;
        var priorityS = settings.getString("emailPriority", EMailPriority.Normal.name());
        EMailPriority priority;
        try {
            priority = EMailPriority.valueOf(priorityS);
        } catch (Exception e) { // NOSONAR backwards compatible loading
            priority = EMailPriority.Normal;
        }
        m_priority = priority;
        m_from = settings.getString("from", getLastUsedHistoryElement(getFromStringHistoryID()));
        m_to = settings.getString("to", System.getProperty("user.name"));
        m_cc = settings.getString("cc", "");
        m_bcc = settings.getString("bcc", "");
        m_replyTo = settings.getString("replyTo", "");
        m_subject = settings.getString("subject", "Workflow finished");
        m_text = settings.getString("text", "");
        String[] urlAsArray = settings.getStringArray("attachedURLs", (String[])null);
        if (urlAsArray == null) {
            urlAsArray = new String[0];
        }
        List<URL> attachedURLsList = new ArrayList<>(urlAsArray.length);
        for (var i = 0; i < urlAsArray.length; i++) {
            URL url;
            try {
                url = new URL(urlAsArray[i]);
            } catch (MalformedURLException ex) {
                try {
                    url = new File(urlAsArray[i]).toURI().toURL();
                } catch (MalformedURLException ex1) {
                    continue; // invalid URL, give up and proceed with next in list
                }
            }
            attachedURLsList.add(url);
        }
        m_attachedURLs = attachedURLsList.toArray(new URL[attachedURLsList.size()]);

        m_smtpConnectionTimeout = settings.getInt("smtpConnectionTimeout", DEFAULT_SMTP_CONNECTION_TIMEOUT);
        m_smtpReadTimeout = settings.getInt("smtpReadTimeout", DEFAULT_SMTP_READ_TIMEOUT);
    }

    /**
     * Load in model.
     *
     * @param settings ..
     * @throws InvalidSettingsException ...
     */
    void loadConfigurationInModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_smtpHost = settings.getString("smtpHost");
        m_smtpPort = settings.getInt("smtpPort");
        m_useAuthentication = settings.getBoolean("useAuthentication");
        m_useCredentials = settings.getBoolean("useCredentials");
        m_credentialsId = settings.getString("credentialsId");
        m_smtpUser = settings.getString("smtpUser");
        if (m_useAuthentication && !m_useCredentials) {
            try {
                // until v2.11 (excl)
                m_smtpPassword = settings.getString("smtpPassword");
            } catch (InvalidSettingsException ise) { // NOSONAR
                m_smtpPassword = settings.getPassword("smtpPasswordWeaklyEncrypted", "0=d#Fs64h");
            }
        } else {
            m_smtpPassword = "";
        }
        m_from = settings.getString("from");
        var connectionSecurityS = settings.getString("connectionSecurity");
        try {
            m_connectionSecurity = ConnectionSecurity.valueOf(connectionSecurityS);
        } catch (Exception e) {
            throw new InvalidSettingsException("Invalid connection security: " + connectionSecurityS, e);
        }
        var formatS = settings.getString("emailFormat");
        try {
            m_format = EMailFormat.valueOf(formatS);
        } catch (Exception e) {
            throw new InvalidSettingsException("Invalid email format: " + formatS, e);
        }
        // added after preview was sent to customer
        var priorityS = settings.getString("emailPriority", EMailPriority.Normal.name());
        try {
            m_priority = EMailPriority.valueOf(priorityS);
        } catch (Exception e) {
            throw new InvalidSettingsException("Invalid email priority: " + priorityS, e);
        }
        m_to = settings.getString("to");
        m_cc = settings.getString("cc");
        m_bcc = settings.getString("bcc");
        // only load if key is contained in settings (for backwards compatibility)
        if (settings.containsKey("replyTo")) {
            m_replyTo = settings.getString("replyTo");
        }
        m_subject = settings.getString("subject");
        m_text = settings.getString("text");
        String[] urlAsArray = settings.getStringArray("attachedURLs", (String[])null);
        if (urlAsArray == null) {
            urlAsArray = new String[0];
        }
        setUrls(urlAsArray);

        m_smtpConnectionTimeout = settings.getInt("smtpConnectionTimeout", DEFAULT_SMTP_CONNECTION_TIMEOUT);
        m_smtpReadTimeout = settings.getInt("smtpReadTimeout", DEFAULT_SMTP_READ_TIMEOUT);
    }

    /**
     * @param urlAsArray
     * @throws InvalidSettingsException
     */
    private void setUrls(final String[] urlAsArray) throws InvalidSettingsException {
        m_attachedURLs = new URL[urlAsArray.length];
        for (var i = 0; i < urlAsArray.length; i++) {
            URL url;
            try {
                url = new URL(urlAsArray[i]);
            } catch (MalformedURLException ex) {
                try {
                    url = new File(urlAsArray[i]).toURI().toURL();
                } catch (MalformedURLException ex1) {
                    throw new InvalidSettingsException("Unparseable URL: " + urlAsArray, ex);
                }
            }
            m_attachedURLs[i] = url;
        }
    }

    /** @return the smtpHost */
    String getSmtpHost() {
        return m_smtpHost;
    }

    /** @param smtpHost the smtpHost to set */
    void setSmtpHost(final String smtpHost) {
        m_smtpHost = smtpHost;
    }

    /** @return the smtpPort */
    int getSmtpPort() {
        return m_smtpPort;
    }

    /** @param smtpPort the smtpPort to set */
    void setSmtpPort(final int smtpPort) {
        m_smtpPort = smtpPort;
    }

    /** @return the useAuthentication */
    boolean isUseAuthentication() {
        return m_useAuthentication;
    }

    /** @param useAuthentication the useAuthentication to set */
    void setUseAuthentication(final boolean useAuthentication) {
        m_useAuthentication = useAuthentication;
    }

    /** @return the useCredentials */
    boolean isUseCredentials() {
        return m_useCredentials;
    }

    /** @param useCredentials the useCredentials to set */
    void setUseCredentials(final boolean useCredentials) {
        m_useCredentials = useCredentials;
    }

    /** @return the credentialsId */
    String getCredentialsId() {
        return m_credentialsId;
    }

    /** @param credentialsId the credentialsId to set */
    void setCredentialsId(final String credentialsId) {
        m_credentialsId = credentialsId;
    }

    /** @return the smtpUser */
    String getSmtpUser() {
        return m_smtpUser;
    }

    /** @param smtpUser the smtpUser to set */
    void setSmtpUser(final String smtpUser) {
        m_smtpUser = smtpUser;
    }

    /** @return the smtpPassword */
    String getSmtpPassword() {
        return m_smtpPassword;
    }

    /** @param smtpPassword the smtpPassword to set */
    void setSmtpPassword(final String smtpPassword) {
        m_smtpPassword = smtpPassword;
    }

    /** @return the connectionSecurity */
    ConnectionSecurity getConnectionSecurity() {
        return m_connectionSecurity;
    }

    /**
     * @param connectionSecurity the connectionSecurity to set
     * @throws InvalidSettingsException if null
     */
    void setConnectionSecurity(final ConnectionSecurity connectionSecurity) throws InvalidSettingsException {
        if (connectionSecurity == null) {
            throw new InvalidSettingsException("connectionSecurity must not be null");
        }
        m_connectionSecurity = connectionSecurity;
    }

    /** @return the from */
    String getFrom() {
        return m_from;
    }

    /** @param from the from to set */
    void setFrom(final String from) {
        m_from = from;
    }

    /** @return the format */
    EMailFormat getFormat() {
        return m_format;
    }

    /**
     * @param format the format to set
     * @throws InvalidSettingsException if null
     */
    void setFormat(final EMailFormat format) throws InvalidSettingsException {
        if (format == null) {
            throw new InvalidSettingsException("format must not be null");
        }
        m_format = format;
    }

    /** @return the priority */
    EMailPriority getPriority() {
        return m_priority;
    }

    /**
     * @param priority the priority to set
     * @throws InvalidSettingsException if null.
     */
    void setPriority(final EMailPriority priority) throws InvalidSettingsException {
        if (priority == null) {
            throw new InvalidSettingsException("priority must not be null");
        }
        m_priority = priority;
    }

    /** @return the to */
    String getTo() {
        return m_to;
    }

    /** @param to the to to set */
    void setTo(final String to) {
        m_to = to;
    }

    /** @return the cc */
    String getCc() {
        return m_cc;
    }

    /** @param cc the cc to set */
    void setCc(final String cc) {
        m_cc = cc;
    }

    /** @return the bcc */
    String getBcc() {
        return m_bcc;
    }

    /** @param bcc the bcc to set */
    void setBcc(final String bcc) {
        m_bcc = bcc;
    }

    /** @return the replyTo */
    String getReplyTo() {
        return m_replyTo;
    }

    /** @param replyTo the replyTo to set */
    void setReplyTo(final String replyTo) {
        m_replyTo = replyTo;
    }

    /** @return the subject */
    String getSubject() {
        return m_subject;
    }

    /** @param subject the subject to set */
    void setSubject(final String subject) {
        m_subject = subject;
    }

    /** @return the text */
    String getText() {
        return m_text;
    }

    /** @param text the text to set */
    void setText(final String text) {
        m_text = text;
    }

    /** @return the attachedURLs */
    URL[] getAttachedURLs() {
        return m_attachedURLs;
    }

    /**
     * @param attachedURLs the attachedURLs to set
     * @throws InvalidSettingsException if null or null elements.
     */
    void setAttachedURLs(final URL[] attachedURLs) throws InvalidSettingsException {
        if (attachedURLs == null || Arrays.asList(attachedURLs).contains(null)) {
            throw new InvalidSettingsException("url list must not be null or contain null elements");
        }
        m_attachedURLs = attachedURLs;
    }

    /**
     * Checks if settings are complete and recipient addresses are OK (according to
     * {@link #PROPERTY_ALLOWED_RECIPIENT_DOMAINS}.
     *
     * @throws InvalidSettingsException Fails if not OK.
     */
    void validateSettings() throws InvalidSettingsException {
        if (getSmtpHost() == null) {
            throw new InvalidSettingsException("No SMTP host specified");
        }
    }

    /**
     * Throws exception if the address list contains forbidden entries according to
     * {@link #PROPERTY_ALLOWED_RECIPIENT_DOMAINS}.
     *
     * @param addressString The non null string as entered in dialog (addresses separated by comma)
     * @return The list of addresses, passed through the validator.
     * @throws AddressException If parsing fails.
     * @thorws InvalidSettingsException If domain not allowed.
     */
    private static InternetAddress[] parseAndValidateRecipients(final String addressString)
        throws InvalidSettingsException, AddressException {
        var validDomainListString = System.getProperty(PROPERTY_ALLOWED_RECIPIENT_DOMAINS);
        InternetAddress[] addressArray = InternetAddress.parse(addressString, false);
        String[] validDomains =
            StringUtils.isEmpty(validDomainListString) ? new String[0] : validDomainListString.toLowerCase().split(",");
        for (InternetAddress a : addressArray) {
            boolean isOK = validDomains.length == 0; // ok if domain list not specified
            final String address = a.getAddress().toLowerCase();
            for (String validDomain : validDomains) {
                isOK = isOK || address.endsWith(validDomain);
            }
            if (!isOK) {
                throw new InvalidSettingsException(String.format(
                    "Recipient '%s' is not valid as the " + "domain is not in the allowed list (system property %s=%s)",
                    address, PROPERTY_ALLOWED_RECIPIENT_DOMAINS, validDomainListString));
            }
        }
        return addressArray;
    }

    /**
     * Send the mail.
     *
     * @throws MessagingException ... when sending fails, also authorization exceptions etc.
     * @throws IOException SSL problems or when copying remote URLs to temp local file.
     * @throws InvalidSettingsException on invalid referenced flow vars
     */
    void send(final FlowVariableProvider flowVarResolver, final CredentialsProvider credProvider)
        throws MessagingException, IOException, InvalidSettingsException {
        final String flowVarCorrectedText = getVarCorrectedText(flowVarResolver);
        var properties = new Properties(System.getProperties());
        final String protocol = addPropertiesAndGetProtocol(properties);

        // Make sure TLS is used. Available protocols can be obtained via:
        // SSLContext.getDefault().getSupportedSSLParameters().getProtocols()
        addMailProtocol(properties, protocol);

        final var session = Session.getInstance(properties, null);
        final MimeMessage message = initMessage(session);

        // text or html message part
        final Multipart mp = initMessageBody(flowVarCorrectedText);

        send(credProvider, protocol, session, message, mp);
    }

    private void send(final CredentialsProvider credProvider, final String protocol, final Session session,
        final MimeMessage message, final Multipart mp)
        throws IOException, InvalidSettingsException, MessagingException {
        List<File> tempDirs = new ArrayList<>();
        Transport t = null;
        var oldContextClassLoader = Thread.currentThread().getContextClassLoader();
        // make sure to set class loader to javax.mail - this has caused problems in the past, see bug 5316
        Thread.currentThread().setContextClassLoader(Session.class.getClassLoader());
        try {
            for (URL url : getAttachedURLs()) {
                addAttachments(mp, tempDirs, url);
            }
            t = sendMail(credProvider, protocol, session, message, mp);
        } catch (MessagingException e) {
            if (e.getCause() instanceof SocketTimeoutException) {
                String timeoutMessage = e.getMessage();
                throw new InvalidSettingsException(timeoutMessage + ", try adjusting the smtp timeout settings", e);
            } else {
                throw new InvalidSettingsException("Error while communicating with the smtp server: " + e, e);
            }
        } finally {
            Thread.currentThread().setContextClassLoader(oldContextClassLoader);
            for (File d : tempDirs) {
                FileUtils.deleteQuietly(d);
            }
            if (t != null) {
                t.close();
            }
        }
    }

    private String getVarCorrectedText(final FlowVariableProvider flowVarResolver) throws InvalidSettingsException {
        try {
            return FlowVariableResolver.parse(m_text, flowVarResolver);
        } catch (NoSuchElementException nse) {
            throw new InvalidSettingsException(nse.getMessage(), nse);
        }
    }

    private String addPropertiesAndGetProtocol(final Properties properties) {
        var protocol = "smtp";
        switch (getConnectionSecurity()) {
            case NONE:
                break;
            case STARTTLS:
                properties.setProperty("mail.smtp.starttls.enable", "true");
                break;
            case SSL:
                // this is the way to do it in javax.mail 1.4.5+ (default is currently (Aug '13) 1.4.0):
                // www.oracle.com/technetwork/java/javamail145sslnotes-1562622.html
                // 'First, and perhaps the simplest, is to set a property to enable use
                // of SSL.  For example, to enable use of SSL for SMTP connections, set
                // the property "mail.smtp.ssl.enable" to "true".'
                properties.setProperty("mail.smtp.ssl.enable", "true");
                // this is an alternative/backup, which works also:
                // http://javakiss.blogspot.ch/2010/10/smtp-in-java-with-javaxmail.html
                // I verify it's actually using SSL:
                //  - it hid a breakpoint in sun.security.ssl.SSLSocketFactoryImpl
                //  - Hostpoint (knime.com mail server) is rejecting any smtp request on their ssl port (465)
                //    without this property
                properties.setProperty("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
                // a third (and most transparent) option would be to use a different protocol:
                protocol = "smtps";
                /* note, protocol smtps doesn't work with default javax.mail bundle (1.4.0):
                 * Unable to load class for provider: protocol=smtps; type=javax.mail.Provider$Type@2d0fc05b;
                 * class=org.apache.geronimo.javamail.transport.smtp.SMTPTSransport;
                 * vendor=Apache Software Foundation;version=1.0
                 * https://issues.apache.org/jira/browse/GERONIMO-4476
                 * It's a typo in geronimo class name (SMTPSTransport vs. SMTPTSransport) - plenty of google hits.
                 * (This impl. uses javax.mail.glassfish bundle.) */
                break;
            default:
        }
        return protocol;
    }

    private void addMailProtocol(final Properties properties, final String protocol) {
        final var mail = "mail.";
        properties.setProperty(mail + "smtp.ssl.protocols", "TLSv1 TLSv1.1 TLSv1.2");

        properties.setProperty(mail + protocol + ".host", getSmtpHost());
        properties.setProperty(mail + protocol + ".port", Integer.toString(getSmtpPort()));
        properties.setProperty(mail + protocol + ".auth", Boolean.toString(isUseAuthentication()));

        /*
         * Use the string value of the timeouts, as some forum posts suggest there are bugs in older JavaMail versions
         * when setting the property with integers.
         */
        properties.setProperty(mail + protocol + ".connectiontimeout", String.valueOf(getSmptConnectionTimeout()));
        properties.setProperty(mail + protocol + ".timeout", String.valueOf(getSmptReadTimeout()));
    }

    private MimeMessage initMessage(final Session session) throws MessagingException, InvalidSettingsException {

        final var message = new MimeMessage(session);

        if (!StringUtils.isBlank(getFrom())) {
            message.setFrom(new InternetAddress(getFrom()));
        } else {
            message.setFrom();
        }
        if (!StringUtils.isBlank(getTo())) {
            message.addRecipients(Message.RecipientType.TO, parseAndValidateRecipients(getTo()));
        }
        if (!StringUtils.isBlank(getCc())) {
            message.addRecipients(Message.RecipientType.CC, parseAndValidateRecipients(getCc()));
        }
        if (!StringUtils.isBlank(getBcc())) {
            message.addRecipients(Message.RecipientType.BCC, parseAndValidateRecipients(getBcc()));
        }
        if (!StringUtils.isBlank(getReplyTo())) {
            message.setReplyTo(parseAndValidateRecipients(getReplyTo()));
        }
        if (message.getAllRecipients() == null) {
            throw new InvalidSettingsException("No recipients specified");
        }
        message.setHeader("X-Mailer", "KNIME/" + KNIMEConstants.VERSION);
        message.setHeader("X-Priority", m_priority.toXPriority());
        message.setSentDate(new Date()); // NOSONAR
        message.setSubject(getSubject(), StandardCharsets.UTF_8.name());

        return message;
    }

    private Multipart initMessageBody(final String flowVarCorrectedText) throws MessagingException {
        var contentBody = new MimeBodyPart();
        String textType;
        switch (getFormat()) {
            case Html:
                textType = "text/html; charset=\"utf-8\"";
                break;
            case Text:
                textType = "text/plain; charset=\"utf-8\"";
                break;
            default:
                throw new RuntimeException("Unsupported format: " + getFormat());
        }
        contentBody.setContent(flowVarCorrectedText, textType);
        Multipart mp = new MimeMultipart();
        mp.addBodyPart(contentBody);
        return mp;
    }

    private static void addAttachments(final Multipart mp, final List<File> tempDirs, final URL url)
        throws IOException, MessagingException {
        var filePart = new MimeBodyPart();
        File file;
        if ("file".equals(url.getProtocol())) {
            try {
                file = new File(url.toURI());
            } catch (URISyntaxException e) {
                throw new IOException("Invalid attachment: " + url, e);
            }
        } else {
            var tempDir = FileUtil.createTempDir("send-mail-attachment");
            tempDirs.add(tempDir);
            try {
                file = new File(tempDir, FilenameUtils.getName(url.toURI().getSchemeSpecificPart()));
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
            FileUtils.copyURLToFile(url, file);
        }
        if (!file.canRead()) {
            throw new IOException("Unable to file attachment \"" + url + "\"");
        }
        filePart.attachFile(file);
        String encodedFileName = MimeUtility.encodeText(file.getName());
        filePart.setFileName(encodedFileName);
        // java 7u7 is missing mimemtypes.default file:
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7096063
        // find mime type in this bundle (META-INF folder contains mime.types) and set it
        filePart.setHeader("Content-Type", FileTypeMap.getDefaultFileTypeMap().getContentType(file));
        mp.addBodyPart(filePart);
    }

    private Transport sendMail(final CredentialsProvider credProvider, final String protocol, final Session session,
        final MimeMessage message, final Multipart mp) throws MessagingException {
        Transport t;
        t = session.getTransport(protocol);
        if (isUseAuthentication()) {
            String user;
            String pass;
            if (isUseCredentials()) {
                var iCredentials = credProvider.get(getCredentialsId());
                user = iCredentials.getLogin();
                pass = iCredentials.getPassword();
            } else {
                user = getSmtpUser();
                pass = getSmtpPassword();
            }
            t.connect(user, pass);
        } else {
            t.connect();
        }
        message.setContent(mp);
        t.sendMessage(message, message.getAllRecipients());
        return t;
    }

    /**
     * Read top most history element, e.g. for smtp host. Used to init defaults.
     *
     * @param historyID ...
     * @return ...
     */
    static String getLastUsedHistoryElement(final String historyID) {
        var instance = StringHistory.getInstance(historyID);
        String[] history = instance.getHistory();
        if (history.length > 0) {
            return history[0];
        }
        return "";
    }

    /** @return smtpHost string history ID. */
    static String getSmtpHostStringHistoryID() {
        return getStringHistoryID("smtpHost");
    }

    /** @return smtpPort string history ID. */
    static String getSmtpPortStringHistoryID() {
        return getStringHistoryID("smtpPort");
    }

    /** @return smtpUser string history ID. */
    static String getSmtpUserStringHistoryID() {
        return getStringHistoryID("smtpUser");
    }

    /** @return from string history ID. */
    static String getFromStringHistoryID() {
        return getStringHistoryID("from");
    }

    /** @return to string history ID. */
    static String getToStringHistoryID() {
        return getStringHistoryID("to");
    }

    /** @return subject string history ID. */
    static String getSubjectStringHistoryID() {
        return getStringHistoryID("subject");
    }

    /** @return attachment list string history ID. */
    static String getAttachmentListStringHistoryID() {
        return getStringHistoryID("attachments");
    }

    private static String getStringHistoryID(final String field) {
        return SendMailConfiguration.class.getPackage().getName() + "-" + field;
    }

    /** @return the smtp connection timeout */
    int getSmptConnectionTimeout() {
        return m_smtpConnectionTimeout;
    }

    /**
     * sets the smtp connection timeout
     *
     * @param smtpConnectionTimeout the connection timeout
     */
    void setSmptConnectionTimeout(final int smtpConnectionTimeout) {
        m_smtpConnectionTimeout = smtpConnectionTimeout;
    }

    /** @return the smtp read timeout */
    int getSmptReadTimeout() {
        return m_smtpReadTimeout;
    }

    /**
     * sets the smtp read timeout
     *
     * @param smtpReadTimeout the connection timeout
     */
    void setSmptReadTimeout(final int smtpReadTimeout) {
        m_smtpReadTimeout = smtpReadTimeout;
    }

}
