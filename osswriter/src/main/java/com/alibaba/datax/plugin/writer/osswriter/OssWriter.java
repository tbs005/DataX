package com.alibaba.datax.plugin.writer.osswriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterUtil;
import com.alibaba.datax.plugin.writer.osswriter.util.OssUtil;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;

/**
 * Created by haiwei.luo on 15-02-09.
 */
public class OssWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;
        private OSSClient ossClient = null;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();
            this.ossClient = OssUtil.initOssClient(this.writerSliceConfig);
        }

        private void validateParameter() {
            this.writerSliceConfig.getNecessaryValue(Key.ENDPOINT,
                    OssWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.ACCESSID,
                    OssWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.ACCESSKEY,
                    OssWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.BUCKET,
                    OssWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.OBJECT,
                    OssWriterErrorCode.REQUIRED_VALUE);
            // warn: do not support compress!!
            String compress = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.COMPRESS);
            if (StringUtils.isNotBlank(compress)) {
                String errorMessage = String.format(
                        "OSS写暂时不支持压缩, 该压缩配置项[%s]不起效用", compress);
                LOG.error(errorMessage);
                throw DataXException.asDataXException(
                        OssWriterErrorCode.ILLEGAL_VALUE, errorMessage);

            }
            UnstructuredStorageWriterUtil
                    .validateParameter(this.writerSliceConfig);

        }

        @Override
        public void prepare() {
            LOG.info("begin do prepare...");
            String bucket = this.writerSliceConfig.getString(Key.BUCKET);
            String object = this.writerSliceConfig.getString(Key.OBJECT);
            String writeMode = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.WRITE_MODE);
            // warn: bucket is not exists, create it
            try {
                // warn: do not create bucket for user
                if (!this.ossClient.doesBucketExist(bucket)) {
                    // this.ossClient.createBucket(bucket);
                    String errorMessage = String.format(
                            "您配置的bucket [%s] 不存在, 请您确认您的配置项.", bucket);
                    LOG.error(errorMessage);
                    throw DataXException.asDataXException(
                            OssWriterErrorCode.ILLEGAL_VALUE, errorMessage);
                }
                LOG.info(String.format("access control details [%s].",
                        this.ossClient.getBucketAcl(bucket).toString()));

                // truncate option handler
                if ("truncate".equals(writeMode)) {
                    LOG.info(String
                            .format("由于您配置了writeMode truncate, 开始清理 [%s] 下面以 [%s] 开头的Object",
                                    bucket, object));
                    // warn: 默认情况下，如果Bucket中的Object数量大于100，则只会返回100个Object
                    while (true) {
                        ObjectListing listing = null;
                        LOG.info("list objects with listObject(bucket, object)");
                        listing = this.ossClient.listObjects(bucket, object);
                        List<OSSObjectSummary> objectSummarys = listing
                                .getObjectSummaries();
                        for (OSSObjectSummary objectSummary : objectSummarys) {
                            LOG.info(String.format("delete oss object [%s].",
                                    objectSummary.getKey()));
                            this.ossClient.deleteObject(bucket,
                                    objectSummary.getKey());
                        }
                        if (objectSummarys.isEmpty()) {
                            break;
                        }
                    }
                } else if ("append".equals(writeMode)) {
                    LOG.info(String
                            .format("由于您配置了writeMode append, 写入前不做清理工作, 数据写入Bucket [%s] 下, 写入相应Object的前缀为  [%s]",
                                    bucket, object));
                } else if ("nonConflict".equals(writeMode)) {
                    LOG.info(String
                            .format("由于您配置了writeMode nonConflict, 开始检查Bucket [%s] 下面以 [%s] 命名开头的Object",
                                    bucket, object));
                    ObjectListing listing = this.ossClient.listObjects(bucket,
                            object);
                    if (0 < listing.getObjectSummaries().size()) {
                        StringBuilder objectKeys = new StringBuilder();
                        objectKeys.append("[ ");
                        for (OSSObjectSummary ossObjectSummary : listing
                                .getObjectSummaries()) {
                            objectKeys.append(ossObjectSummary.getKey() + " ,");
                        }
                        objectKeys.append(" ]");
                        LOG.info(String.format(
                                "object with prefix [%s] details: %s", object,
                                objectKeys.toString()));
                        throw DataXException
                                .asDataXException(
                                        OssWriterErrorCode.ILLEGAL_VALUE,
                                        String.format(
                                                "您配置的Bucket: [%s] 下面存在其Object有前缀 [%s].",
                                                bucket, object));
                    }
                }
            } catch (OSSException e) {
                throw DataXException.asDataXException(
                        OssWriterErrorCode.OSS_COMM_ERROR, e.getMessage());
            } catch (ClientException e) {
                throw DataXException.asDataXException(
                        OssWriterErrorCode.OSS_COMM_ERROR, e.getMessage());
            }
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOG.info("begin do split...");
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            String object = this.writerSliceConfig.getString(Key.OBJECT);
            String bucket = this.writerSliceConfig.getString(Key.BUCKET);

            Set<String> allObjects = new HashSet<String>();
            try {
                List<OSSObjectSummary> ossObjectlisting = this.ossClient
                        .listObjects(bucket).getObjectSummaries();
                for (OSSObjectSummary objectSummary : ossObjectlisting) {
                    allObjects.add(objectSummary.getKey());
                }
            } catch (OSSException e) {
                throw DataXException.asDataXException(
                        OssWriterErrorCode.OSS_COMM_ERROR, e.getMessage());
            } catch (ClientException e) {
                throw DataXException.asDataXException(
                        OssWriterErrorCode.OSS_COMM_ERROR, e.getMessage());
            }

            String objectSuffix;
            for (int i = 0; i < mandatoryNumber; i++) {
                // handle same object name
                Configuration splitedTaskConfig = this.writerSliceConfig
                        .clone();

                String fullObjectName = null;
                objectSuffix = UUID.randomUUID().toString().replace('-', '_');
                fullObjectName = String.format("%s__%s", object, objectSuffix);
                while (allObjects.contains(fullObjectName)) {
                    objectSuffix = UUID.randomUUID().toString()
                            .replace('-', '_');
                    fullObjectName = String.format("%s__%s", object,
                            objectSuffix);
                }
                allObjects.add(fullObjectName);

                splitedTaskConfig.set(Key.OBJECT, fullObjectName);

                LOG.info(String.format("splited write object name:[%s]",
                        fullObjectName));

                writerSplitConfigs.add(splitedTaskConfig);
            }
            LOG.info("end do split.");
            return writerSplitConfigs;
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private OSSClient ossClient;
        private Configuration writerSliceConfig;
        private String bucket;
        private String object;
        private String nullFormat;
        private String encoding;
        private char fieldDelimiter;
        private String dateFormat;
        private String fileFormat;
        private List<String> header;
        private Long maxFileSize;//MB

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.ossClient = OssUtil.initOssClient(this.writerSliceConfig);
            this.bucket = this.writerSliceConfig.getString(Key.BUCKET);
            this.object = this.writerSliceConfig.getString(Key.OBJECT);
            this.nullFormat = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.NULL_FORMAT);
            this.dateFormat = this.writerSliceConfig
                    .getString(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.DATE_FORMAT,
                            null);
            this.encoding = this.writerSliceConfig
                    .getString(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.ENCODING,
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.DEFAULT_ENCODING);
            this.fieldDelimiter = this.writerSliceConfig
                    .getChar(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FIELD_DELIMITER,
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.DEFAULT_FIELD_DELIMITER);
            this.fileFormat = this.writerSliceConfig
                    .getString(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_FORMAT,
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.FILE_FORMAT_TEXT);
            this.header = this.writerSliceConfig
                    .getList(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.HEADER,
                            null, String.class);
            this.maxFileSize = this.writerSliceConfig
                    .getLong(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.MAX_FILE_SIZE,
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.MAX_FILE_SIZE);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            // 设置每块字符串长度
            final long partSize = 1024 * 1024 * 10L;
            long numberCacul = (this.maxFileSize * 1024 * 1024L) / partSize;
            final long maxPartNumber = numberCacul >= 1 ? numberCacul : 1;
            int objectSufix = 0;
            StringBuilder sb = new StringBuilder();
            Record record = null;

            LOG.info(String.format(
                    "begin do write, each object maxFileSize: [%s]MB...",
                    maxPartNumber * 10));
            String currentObject = this.object;
            InitiateMultipartUploadRequest currentInitiateMultipartUploadRequest = null;
            InitiateMultipartUploadResult currentInitiateMultipartUploadResult = null;
            List<PartETag> currentPartETags = null;
            // to do: 可以根据currentPartNumber做分块级别的重试，InitiateMultipartUploadRequest多次一个currentPartNumber会覆盖原有
            int currentPartNumber = 1;
            try {
                // warn
                boolean needInitMultipartTransform = true;
                while ((record = lineReceiver.getFromReader()) != null) {
                    // init:begin new multipart upload
                    if (needInitMultipartTransform) {
                        if (objectSufix == 0) {
                            currentObject = this.object;
                        } else {
                            currentObject = String.format("%s_%s", this.object,
                                    objectSufix);
                        }
                        objectSufix++;
                        currentInitiateMultipartUploadRequest = new InitiateMultipartUploadRequest(
                                this.bucket, currentObject);
                        currentInitiateMultipartUploadResult = this.ossClient
                                .initiateMultipartUpload(currentInitiateMultipartUploadRequest);
                        currentPartETags = new ArrayList<PartETag>();
                        LOG.info(String
                                .format("write to bucket: [%s] object: [%s] with oss uploadId: [%s]",
                                        this.bucket, currentObject,
                                        currentInitiateMultipartUploadResult
                                                .getUploadId()));

                        // each object's header
                        if (null != this.header && !this.header.isEmpty()) {
                            sb.append(UnstructuredStorageWriterUtil
                                    .doTransportOneRecord(this.header,
                                            this.fieldDelimiter,
                                            this.fileFormat));
                        }
                        // warn
                        needInitMultipartTransform = false;
                        currentPartNumber = 1;
                    }

                    // write: upload data to current object
                    MutablePair<String, Boolean> transportResult = UnstructuredStorageWriterUtil
                            .transportOneRecord(record, nullFormat, dateFormat,
                                    fieldDelimiter, this.fileFormat,
                                    this.getTaskPluginCollector());
                    if (!transportResult.getRight()) {
                        sb.append(transportResult.getLeft());
                    }

                    if (sb.length() >= partSize) {
                        this.uploadOnePart(sb, currentPartNumber,
                                currentInitiateMultipartUploadResult,
                                currentPartETags, currentObject);
                        currentPartNumber++;
                        sb.setLength(0);
                    }

                    // save: end current multipart upload
                    if (currentPartNumber > maxPartNumber) {
                        LOG.info(String
                                .format("current object [%s] size > %s, complete current multipart upload and begin new one",
                                        currentObject, currentPartNumber * partSize));
                        CompleteMultipartUploadRequest currentCompleteMultipartUploadRequest = new CompleteMultipartUploadRequest(
                                this.bucket, currentObject,
                                currentInitiateMultipartUploadResult
                                        .getUploadId(), currentPartETags);
                        CompleteMultipartUploadResult currentCompleteMultipartUploadResult = this.ossClient
                                .completeMultipartUpload(currentCompleteMultipartUploadRequest);
                        LOG.info(String.format(
                                "final object [%s] etag is:[%s]",
                                currentObject,
                                currentCompleteMultipartUploadResult.getETag()));
                        // warn
                        needInitMultipartTransform = true;
                    }
                }

                // warn: may be some data stall in sb
                if (0 < sb.length()) {
                    this.uploadOnePart(sb, currentPartNumber,
                            currentInitiateMultipartUploadResult,
                            currentPartETags, currentObject);
                }
                CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(
                        this.bucket, currentObject,
                        currentInitiateMultipartUploadResult.getUploadId(),
                        currentPartETags);
                CompleteMultipartUploadResult completeMultipartUploadResult = this.ossClient
                        .completeMultipartUpload(completeMultipartUploadRequest);
                LOG.info(String.format("final object etag is:[%s]",
                        completeMultipartUploadResult.getETag()));

            } catch (UnsupportedEncodingException e) {
                throw DataXException.asDataXException(
                        OssWriterErrorCode.ILLEGAL_VALUE,
                        String.format("不支持您配置的编码格式:[%s]", encoding));
            } catch (OSSException e) {
                throw DataXException.asDataXException(
                        OssWriterErrorCode.Write_OBJECT_ERROR, e.getMessage());
            } catch (ClientException e) {
                throw DataXException.asDataXException(
                        OssWriterErrorCode.Write_OBJECT_ERROR, e.getMessage());
            } catch (IOException e) {
                throw DataXException.asDataXException(
                        OssWriterErrorCode.Write_OBJECT_ERROR, e.getMessage());
            }
            LOG.info("end do write");
        }

        private void uploadOnePart(StringBuilder sb, int partNumber,
                InitiateMultipartUploadResult initiateMultipartUploadResult,
                List<PartETag> partETags, String currentObject)
                throws IOException, UnsupportedEncodingException {
            byte[] byteArray = sb.toString().getBytes(this.encoding);
            InputStream inputStream = new ByteArrayInputStream(byteArray);
            // 创建UploadPartRequest，上传分块
            UploadPartRequest uploadPartRequest = new UploadPartRequest();
            uploadPartRequest.setBucketName(this.bucket);
            uploadPartRequest.setKey(currentObject);
            uploadPartRequest.setUploadId(initiateMultipartUploadResult
                    .getUploadId());
            uploadPartRequest.setInputStream(inputStream);
            uploadPartRequest.setPartSize(byteArray.length);
            uploadPartRequest.setPartNumber(partNumber);
            UploadPartResult uploadPartResult = this.ossClient
                    .uploadPart(uploadPartRequest);
            partETags.add(uploadPartResult.getPartETag());
            LOG.info(String.format(
                    "upload part [%s] size [%s] Byte has been completed.",
                    partNumber, byteArray.length));
            inputStream.close();
        }

        @Override
        public void prepare() {

        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }
    }
}
