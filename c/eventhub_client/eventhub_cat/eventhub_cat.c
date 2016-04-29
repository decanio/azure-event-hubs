// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#include <stdlib.h>
#include <stdio.h>

#include <time.h>
#include <signal.h>

#include <proton/message.h>
#include <proton/messenger.h>
#include <proton/error.h>

#include "eventhubclient.h"
#include "eventdata.h"
#include "eventhub_testclient.h"
#include "eventhub_cat.h"
#include "threadapi.h"
#include "crt_abstractions.h"
#include "platform.h"

static const char *connectionString = "amqps://listen:{Listen key}@{namespace name}.servicebus.windows.net/{eventhub name}/ConsumerGroups/$default/Partitions/%u";

typedef struct EVENTHUB_TEST_CLIENT_INFO_TAG
{
    STRING_HANDLE eventHubName;
    char* svcBusName;
    char* manageKey;
    char* hostName;
    THREAD_HANDLE hThread;
    volatile int messageThreadExit;
    unsigned int partitionCount;
} EVENTHUB_TEST_CLIENT_INFO;

#define EVENTHUB_TEST_CLIENT_RESULT_VALUES \
    EVENTHUB_TEST_CLIENT_OK, \
    EVENTHUB_TEST_CLIENT_INVALID_ARG, \
    EVENTHUB_TEST_CLIENT_SEND_NOT_SUCCESSFUL, \
    EVENTHUB_TEST_CLIENT_ERROR

/*the following time expressed in seconds denotes the maximum time to read all the events available in an event hub*/
#define MAX_EXECUTE_TIME            60.0

#define PROTON_DEFAULT_TIMEOUT      (3*1000)

#define THREAD_CONTINUE             0
#define THREAD_END                  1

#define MAX_PARTITION_SIZE          16

const char* EventHubAccount_GetConnectionString(void)
{
    const char* envVar = getenv("EVENTHUB_CONNECTION_STRING");
    if (envVar == NULL)
    {
        fprintf(stderr, "Failed: EVENTHUB_CONNECTION_STRING is NULL\r\n");
    }
    return envVar;
}

const char* EventHubAccount_GetName(void)
{
    const char* envVar = getenv("EVENTHUB_NAME");
    if (envVar == NULL)
    {
        fprintf(stderr, "Failed: EVENTHUB_NAME is NULL\r\n");
    }
    return envVar;
}

int EventHubAccount_PartitionCount(void)
{
    int nPartitionCount;
    char* envVar = getenv("EVENTHUB_PARTITION_COUNT");
    if (envVar == NULL)
    {
        fprintf(stderr, "Warning: EVENTHUB_PARTITION_COUNT is NULL using value of %d\r\n", MAX_PARTITION_SIZE);
        nPartitionCount = MAX_PARTITION_SIZE;
    }
    else
    {
        nPartitionCount = atoi(envVar);
    }
    return nPartitionCount;
}

EVENTHUB_TEST_CLIENT_HANDLE EventHub_Initialize(const char* pszconnString, const char* pszeventHubName)
{
    EVENTHUB_TEST_CLIENT_HANDLE result;
    EVENTHUB_TEST_CLIENT_INFO* evhInfo;

#ifdef NOTYET
    if (pszconnString == NULL || pszeventHubName == NULL)
    {
        result = NULL;
    }
    else
#endif
    if ((evhInfo = (EVENTHUB_TEST_CLIENT_INFO*)malloc(sizeof(EVENTHUB_TEST_CLIENT_INFO))) == NULL)
    {
        result = NULL;
    }
#ifdef NOTYET
    else if ((evhInfo->eventHubName = STRING_construct(pszeventHubName)) == NULL)
    {
        free(evhInfo);
        result = NULL;
    }
    else if (RetrieveEventHubClientInfo(pszconnString, evhInfo) != 0)
    {
        STRING_delete(evhInfo->eventHubName);
        free(evhInfo);
        result = NULL;
    }
#endif
    else
    {
        evhInfo->hThread = NULL;
        evhInfo->messageThreadExit = THREAD_CONTINUE;
        evhInfo->partitionCount = 0;
        result = evhInfo;
    }
    return result;
}

typedef struct EXPECTED_RECEIVE_DATA_TAG
{
    int x;
} EXPECTED_RECEIVE_DATA;

static int EventhubClientCallback(void* context, const char* data, size_t size)
{
    int result = 0; /* 0 means keep processing data */

    fprintf(stdout, "%s\n", data);

    return result;
}

/*this function will read messages from EventHub until exhaustion of all the messages (as indicated by a timeout in the proton messenger)*/
/*while reading the messages, it will invoke a callback. If the callback msgCallback is NULL, it will simply not call it*/
/*when the function returns all the existing messages have been consumed (given to the callback or simply skipped)*/
/*the callback shall return "0" if more messages are to be processed or "1" if no more messages are to be processed*/
/*the whole operation is time-bound*/
EVENTHUB_TEST_CLIENT_RESULT EventHub_ListenForMsg(EVENTHUB_TEST_CLIENT_HANDLE eventhubHandle, pfEventHubMessageCallback msgCallback, int partitionCount, double maxExecutionTimePerPartition, void* context)
{
    EVENTHUB_TEST_CLIENT_RESULT result;

    if (eventhubHandle == NULL) {
        return EVENTHUB_TEST_CLIENT_INVALID_ARG;
    }
        
    EVENTHUB_TEST_CLIENT_INFO* evhInfo = (EVENTHUB_TEST_CLIENT_INFO*)eventhubHandle;
    time_t beginExecutionTime, nowExecutionTime;
    double timespan;
    pn_messenger_t* messenger;
    if ((messenger = pn_messenger(NULL)) == NULL) { 
        return EVENTHUB_TEST_CLIENT_ERROR;
    }
    // Sets the Messenger Windows
    pn_messenger_set_incoming_window(messenger, 1);

    pn_messenger_start(messenger);
    if (pn_messenger_errno(messenger)) {
        return EVENTHUB_TEST_CLIENT_ERROR;
    }
    if (pn_messenger_set_timeout(messenger, PROTON_DEFAULT_TIMEOUT) != 0) {
        return EVENTHUB_TEST_CLIENT_ERROR;
    }
    result = EVENTHUB_TEST_CLIENT_OK;

    evhInfo->partitionCount = partitionCount;
    evhInfo->messageThreadExit = THREAD_CONTINUE;

    int addressLen = strlen(connectionString);
    char* szAddress = (char*)malloc(addressLen + 1);
    if (szAddress == NULL) {
        printf("error in malloc\r\n");
        return EVENTHUB_TEST_CLIENT_ERROR;
    }

    size_t part;
    int pnErrorNo = 0;
    int queueDepth = 0;
    const char* eventhubName = STRING_c_str(evhInfo->eventHubName);

    /*subscribe the messenger to all the partitions*/
    for (part = 0; part < evhInfo->partitionCount; part++) {
        sprintf_s(szAddress, addressLen + 1, connectionString, part);
        pn_messenger_subscribe(messenger, szAddress);
    }

    bool atLeastOneMessageReceived = true;
    beginExecutionTime = time(NULL);
    while ( (atLeastOneMessageReceived) &&
            (evhInfo->messageThreadExit == THREAD_CONTINUE) &&
            ((nowExecutionTime = time(NULL)), timespan = difftime(nowExecutionTime, beginExecutionTime), timespan < maxExecutionTimePerPartition) ) {

        atLeastOneMessageReceived = false;
        // Wait for the message to be recieved
        pn_messenger_recv(messenger, -1);
        if ( (pnErrorNo = pn_messenger_errno(messenger) ) != 0) {
            fprintf(stderr, "Error found on pn_messenger_recv: %d\r\n", pnErrorNo);
            break;
        } else {
            while ( (evhInfo->messageThreadExit == THREAD_CONTINUE) &&
                    (queueDepth = pn_messenger_incoming(messenger) ) > 0) {
                pn_message_t* pnMessage = pn_message();
                if (pnMessage == NULL) {
                    evhInfo->messageThreadExit = THREAD_END;
                } else {
                    if (pn_messenger_get(messenger, pnMessage) != 0) {
                        evhInfo->messageThreadExit = THREAD_END;
                        break;
                    } else {
                        pn_tracker_t tracker = pn_messenger_incoming_tracker(messenger);

                        pn_data_t* pBody = pn_message_body(pnMessage);
                        if (pBody != NULL) {
                            if (!pn_data_next(pBody)) {
                                evhInfo->messageThreadExit = THREAD_END;
                            } else {
                                pn_type_t typeOfBody = pn_data_type(pBody);
                                atLeastOneMessageReceived = true;
                                if (PN_STRING == typeOfBody) {
                                    pn_bytes_t descriptor = pn_data_get_string(pBody);
                                    if (msgCallback != NULL) {
                                        if (msgCallback(context, descriptor.start, descriptor.size) != 0) {
                                            evhInfo->messageThreadExit = THREAD_END;
                                        }
                                    }
                                } else if (PN_BINARY == typeOfBody) {
                                    pn_bytes_t descriptor = pn_data_get_binary(pBody);
                                    if (msgCallback != NULL) {
                                        if (msgCallback(context, descriptor.start, descriptor.size) != 0) {
                                            evhInfo->messageThreadExit = THREAD_END;
                                        }
                                    }
                                } else {
                                    //Unknown Data Item
                                }
                            }
                        }
                        pn_messenger_accept(messenger, tracker, 0);
                        pn_message_clear(pnMessage);
                        pn_messenger_settle(messenger, tracker, 0);
                    }

                    pn_message_free(pnMessage);
                }
            }
        }
    }
    free(szAddress);

    /*bring down the messenger*/
    while (!pn_messenger_stopped(messenger))
    {
        pn_messenger_stop(messenger);
    }
    pn_messenger_free(messenger);

    return result;
}

int EventHub_Cat(void)
{
    EXPECTED_RECEIVE_DATA messageData;
    int result;

    if (platform_init() != 0) {
        (void)printf("ERROR: Failed initializing platform!\r\n");
        result = 1;
        return result;
    }

    EVENTHUB_TEST_CLIENT_HANDLE eventhubTestHandle = EventHub_Initialize(EventHubAccount_GetConnectionString(), EventHubAccount_GetName() );

    EVENTHUB_TEST_CLIENT_RESULT eventTestResult = EventHub_ListenForMsg(eventhubTestHandle, EventhubClientCallback, EventHubAccount_PartitionCount(), MAX_EXECUTE_TIME, &messageData);

    platform_deinit();

    return result; 
}
