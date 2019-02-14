package com.alibaba.datax.core.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConfigParserTest extends CaseInitializer {
    private String jobPath;

    @Before
    public void setUp() {
        String path = ConfigParserTest.class.getClassLoader()
                .getResource(".").getFile();
        this.jobPath = path + File.separator
                + "job" + File.separator + "job.json";
    }
	@Test
	public void test() throws URISyntaxException {
		Configuration configuration = ConfigParser.parse(jobPath);
		System.out.println(configuration.toJSON());

		Assert.assertTrue(configuration.getList("job.content").size() == 2);
		Assert.assertTrue(configuration.getString("job.content[0].reader.name")
				.equals("fakereader"));
		Assert.assertTrue(configuration.getString("job.content[1].reader.name")
				.equals("fakereader"));
		Assert.assertTrue(configuration.getString("job.content[0].writer.name")
				.equals("fakewriter"));
		Assert.assertTrue(configuration.getString("job.content[1].writer.name")
				.equals("fakewriter"));

		System.out.println(configuration.getConfiguration("plugin").toJSON());

		configuration = configuration.getConfiguration("plugin");
		Assert.assertTrue(configuration.getString("reader.fakereader.name")
				.equals("fakereader"));
		Assert.assertTrue(configuration.getString("writer.fakewriter.name")
				.equals("fakewriter"));
	}

    @Test
    public void secretTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String password = "password";
        String accessKey = "accessKey";
        String readerParamPath =
                "job.content[0].reader.parameter";
        String writerParamPath =
                "job.content[1].writer.parameter";

        Map<String, String> secretMap = getPublicKeyMap();
        String keyVersion = null;
        for(String version : secretMap.keySet()) {
            keyVersion = version;
            break;
        }

        Configuration config = ConfigParser.parse(jobPath);
        config.set(CoreConstant.DATAX_JOB_SETTING_KEYVERSION,
                keyVersion);
        config.set(readerParamPath+".*password",
                SecretUtil.encrypt(password, secretMap.get(keyVersion), SecretUtil.KEY_ALGORITHM_RSA));
        config.set(readerParamPath+".*long", 100);
        config.set(writerParamPath+".*accessKey",
                SecretUtil.encrypt(accessKey, secretMap.get(keyVersion), SecretUtil.KEY_ALGORITHM_RSA));
        config.set(writerParamPath+".*long", 200);

        config = SecretUtil.decryptSecretKey(config);

        Assert.assertTrue(password.equals(
                config.getString(readerParamPath+".password")));
        Assert.assertTrue(config.isSecretPath(
                readerParamPath+".password"));
        Assert.assertTrue(config.get(readerParamPath+".*long") != null);
        Assert.assertTrue(accessKey.equals(
                config.getString(writerParamPath+".accessKey")));
        Assert.assertTrue(config.isSecretPath(
                writerParamPath+".accessKey"));
        Assert.assertTrue(config.get(writerParamPath+".*long") != null);
        Assert.assertTrue(StringUtils.isBlank(
                config.getString(readerParamPath+".*password")));
        Assert.assertTrue(StringUtils.isBlank(
                config.getString(writerParamPath+".*accessKey")));
    }

    private Map<String, String> getPublicKeyMap() {
        Map<String, String> versionKeyMap =
                new HashMap<String, String>();
        InputStream secretStream = null;
        try {
            secretStream = new FileInputStream(
                    CoreConstant.DATAX_SECRET_PATH);
        } catch (FileNotFoundException e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR,
                    "DataX配置要求加解密，但无法找到密钥的配置文件");
        }

        Properties properties = new Properties();
        try {
            properties.load(secretStream);
            secretStream.close();
        } catch (IOException e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR, "读取加解密配置文件出错", e);
        }

        String lastKeyVersion = properties.getProperty(
                CoreConstant.LAST_KEYVERSION);
        String lastPublicKey = properties.getProperty(
                CoreConstant.LAST_PUBLICKEY);
        String lastPrivateKey = properties.getProperty(
                CoreConstant.LAST_PRIVATEKEY);
        if(StringUtils.isNotBlank(lastKeyVersion)) {
            if(StringUtils.isBlank(lastPublicKey) ||
                    StringUtils.isBlank(lastPrivateKey)) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.SECRET_ERROR,
                        "DataX配置要求加解密，但上次配置的公私钥对存在为空的情况"
                );
            }

            versionKeyMap.put(lastKeyVersion, lastPublicKey);
        }

        String currentKeyVersion = properties.getProperty(
                CoreConstant.CURRENT_KEYVERSION);
        String currentPublicKey = properties.getProperty(
                CoreConstant.CURRENT_PUBLICKEY);
        String currentPrivateKey = properties.getProperty(
                CoreConstant.CURRENT_PRIVATEKEY);
        if(StringUtils.isNotBlank(currentKeyVersion)) {
            if(StringUtils.isBlank(currentPublicKey) ||
                    StringUtils.isBlank(currentPrivateKey)) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.SECRET_ERROR,
                        "DataX配置要求加解密，但当前配置的公私钥对存在为空的情况");
            }

            versionKeyMap.put(currentKeyVersion, currentPublicKey);
        }

        if(versionKeyMap.size() <= 0) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.SECRET_ERROR,
                    "DataX配置要求加解密，但无法找到公私钥");
        }

        return versionKeyMap;
    }
}
