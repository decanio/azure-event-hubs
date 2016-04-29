#Copyright (c) Microsoft. All rights reserved.
#Licensed under the MIT license. See LICENSE file in the project root for full license information.

#this is CMakeLists.txt for eventhub_cat

compileAsC99()

set(eh_c_files
    ./eventhub_cat.c
)

if(WIN32)
    set(eh_c_files ${eh_c_files} ./windows/main.c)
else()
    set(eh_c_files ${eh_c_files} ./linux/main.c)
endif()

set(eh_h_files
    eventhub_cat.h
)

IF(WIN32)
    #windows needs this define
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
    add_definitions(-DGB_MEASURE_MEMORY_FOR_THIS -DGB_DEBUG_ALLOC)
ENDIF(WIN32)

include_directories(. ../../testtools/eventhub_test/inc)

add_executable(eventhub_cat ${eh_c_files} ${eh_h_files})

target_link_libraries(eventhub_cat
        eventhub_client uamqp qpid-proton)

linkSharedUtil(eventhub_cat)

