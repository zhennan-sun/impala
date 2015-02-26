# - Find LZ4 (lz4.h, liblz4.a, liblz4.so, and liblz4.so.1)
# This module defines
# LZ4_INCLUDE_DIR, directory containing headers
# LZ4_LIBS, directory containing lz4 libraries
# LZ4_STATIC_LIB, path to liblz4.a
# LZ4_FOUND, whether lz4 has been found

set(LZ4_SEARCH_HEADER_PATHS
  ${CMAKE_SOURCE_DIR}/thirdparty/lz4
)

set(LZ4_SEARCH_LIB_PATH
  ${CMAKE_SOURCE_DIR}/thirdparty/lz4
)

set(LZ4_INCLUDE_DIR
  ${CMAKE_SOURCE_DIR}/thirdparty/lz4
)

find_library(LZ4_LIB_PATH NAMES lz4
  PATHS ${LZ4_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
)

if (LZ4_LIB_PATH)
  set(LZ4_FOUND TRUE)
  set(LZ4_LIBS ${LZ4_SEARCH_LIB_PATH})
  set(LZ4_STATIC_LIB ${LZ4_SEARCH_LIB_PATH}/liblz4.a)
else ()
  set(LZ4_FOUND FALSE)
endif ()

if (LZ4_FOUND)
  if (NOT LZ4_FIND_QUIETLY)
    message(STATUS "Lz4 Found in ${LZ4_SEARCH_LIB_PATH}")
  endif ()
else ()
  message(STATUS "Lz4 includes and libraries NOT found. "
    "Looked for headers in ${LZ4_SEARCH_HEADER_PATH}, "
    "and for libs in ${LZ4_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  LZ4_INCLUDE_DIR
  LZ4_LIBS
  LZ4_STATIC_LIB
)
