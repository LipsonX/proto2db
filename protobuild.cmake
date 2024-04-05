macro(add_protolib libname proto_dir proto_out cmdenv protoc)
    message("add_protolib ${libname}")

    if (NOT EXISTS ${proto_out})
        file(MAKE_DIRECTORY ${proto_out})
    endif ()


    file(GLOB_RECURSE MSG_PROTOS ${proto_dir}/*.proto)
    set(PROTO_SRC)
    foreach(proto_file ${MSG_PROTOS})
        get_filename_component(FILE_NAME ${proto_file} NAME_WE)
        get_filename_component(FILE_PATH_ABS ${proto_file} ABSOLUTE)
        string(REPLACE "${proto_dir}" "${proto_out}" FILE_PATH_REL ${FILE_PATH_ABS})

        string(REPLACE ".proto" ".pb.cc" FILE_NAME_CC ${FILE_PATH_REL})
        string(REPLACE ".proto" ".pb.h" FILE_NAME_H ${FILE_PATH_REL})
        message("pb file ${proto_file} ${FILE_NAME_CC}")

        add_custom_command(
                OUTPUT ${FILE_NAME_CC} ${FILE_NAME_H}
                COMMAND ${CMAKE_COMMAND} -E env "${cmdenv}:$ENV{LD_LIBRARY_PATH}"
                ${protoc} ARGS --cpp_out ${proto_out} -I ${proto_dir} ${proto_file}
                DEPENDS ${proto_file}
                COMMENT "Running C++ protocol buffer compiler on ${proto_file}"
        )
        set(PROTO_SRC ${PROTO_SRC} ${FILE_NAME_CC})
    endforeach()

    add_library(${libname} SHARED ${PROTO_SRC})
    target_link_libraries(${libname} protobuf pthread)
    include_directories(${proto_out})
endmacro(add_protolib)