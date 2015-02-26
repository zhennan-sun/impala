# - Find Thrift (a cross platform RPC lib/tool)
# This module defines
<<<<<<< HEAD
#  Thrift_VERSION, version string of ant if found
#  Thrift_INCLUDE_DIR, where to find Thrift headers
#  Thrift_CONTRIB_DIR, where contrib thrift files (e.g. fb303.thrift) are installed
#  Thrift_LIBS, Thrift libraries
#  Thrift_FOUND, If false, do not try to use ant


# prefer the thrift version supplied in THRIFT_HOME
message(STATUS "$ENV{THRIFT_HOME}")
find_path(Thrift_INCLUDE_DIR Thrift.h HINTS
  $ENV{THRIFT_HOME}/include/thrift
  /usr/local/include/thrift
  /opt/local/include/thrift
)

# Use the default install dir of thrift contrib (/usr/local)
# if env var THRIFT_CONTRIB_DIR is not set
set(Thrift_CONTRIB_DIR $ENV{THRIFT_CONTRIB_DIR})
IF (NOT Thrift_CONTRIB_DIR)
  set(Thrift_CONTRIB_DIR /usr/local)
ENDIF (NOT Thrift_CONTRIB_DIR)

set(Thrift_LIB_PATHS
=======
#  THRIFT_VERSION, version string of ant if found
#  THRIFT_INCLUDE_DIR, where to find THRIFT headers
#  THRIFT_CONTRIB_DIR, where contrib thrift files (e.g. fb303.thrift) are installed
#  THRIFT_LIBS, THRIFT libraries
#  THRIFT_FOUND, If false, do not try to use ant

# prefer the thrift version supplied in THRIFT_HOME
message(STATUS "THRIFT_HOME: $ENV{THRIFT_HOME}")
find_path(THRIFT_INCLUDE_DIR thrift/Thrift.h HINTS
  $ENV{THRIFT_HOME}/include/
  /usr/local/include/
  /opt/local/include/
)

find_path(THRIFT_CONTRIB_DIR share/fb303/if/fb303.thrift HINTS
  $ENV{THRIFT_HOME}
  /usr/local/
)

set(THRIFT_LIB_PATHS
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  $ENV{THRIFT_HOME}/lib
  /usr/local/lib
  /opt/local/lib)

<<<<<<< HEAD
find_path(Thrift_STATIC_LIB_PATH libthrift.a PATHS ${Thrift_LIB_PATHS})

# prefer the thrift version supplied in THRIFT_HOME
find_library(Thrift_LIB NAMES thrift HINTS ${Thrift_LIB_PATHS})

find_path(THRIFT_COMPILER_PATH NAMES thrift PATHS
  $ENV{THRIFT_HOME}/bin
  /usr/local/bin
  /usr/bin
)

if (Thrift_LIB)
  set(Thrift_FOUND TRUE)
  set(Thrift_LIBS ${Thrift_LIB})
  set(Thrift_STATIC_LIB ${Thrift_STATIC_LIB_PATH}/libthrift.a)
  set(Thrift_NB_STATIC_LIB ${Thrift_STATIC_LIB_PATH}/libthriftnb.a)
  set(Thrift_COMPILER ${THRIFT_COMPILER_PATH}/thrift)
  exec_program(${Thrift_COMPILER}
    ARGS -version OUTPUT_VARIABLE Thrift_VERSION RETURN_VALUE Thrift_RETURN)
else ()
  set(Thrift_FOUND FALSE)
endif ()

if (Thrift_FOUND)
  if (NOT Thrift_FIND_QUIETLY)
    message(STATUS "${Thrift_VERSION}")
  endif ()
else ()
  message(STATUS "Thrift compiler/libraries NOT found. "
          "Thrift support will be disabled (${Thrift_RETURN}, "
          "${Thrift_INCLUDE_DIR}, ${Thrift_LIB})")
=======
find_path(THRIFT_STATIC_LIB_PATH libthrift.a PATHS ${THRIFT_LIB_PATHS})

# prefer the thrift version supplied in THRIFT_HOME
find_library(THRIFT_LIB NAMES thrift HINTS ${THRIFT_LIB_PATHS})

find_program(THRIFT_COMPILER thrift
  $ENV{THRIFT_HOME}/bin
  /usr/local/bin
  /usr/bin
  NO_DEFAULT_PATH
)

if (THRIFT_LIB)
  set(THRIFT_FOUND TRUE)
  set(THRIFT_LIBS ${THRIFT_LIB})
  set(THRIFT_STATIC_LIB ${THRIFT_STATIC_LIB_PATH}/libthrift.a)
  exec_program(${THRIFT_COMPILER}
    ARGS -version OUTPUT_VARIABLE THRIFT_VERSION RETURN_VALUE THRIFT_RETURN)
else ()
  set(THRIFT_FOUND FALSE)
endif ()

if (THRIFT_FOUND)
  if (NOT THRIFT_FIND_QUIETLY)
    message(STATUS "Thrift version: ${THRIFT_VERSION}")
  endif ()
else ()
  message(STATUS "Thrift compiler/libraries NOT found. "
          "Thrift support will be disabled (${THRIFT_RETURN}, "
          "${THRIFT_INCLUDE_DIR}, ${THRIFT_LIB})")
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
endif ()


mark_as_advanced(
<<<<<<< HEAD
  Thrift_LIB
  Thrift_COMPILER
  Thrift_INCLUDE_DIR
=======
  THRIFT_LIB
  THRIFT_COMPILER
  THRIFT_INCLUDE_DIR
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
)
