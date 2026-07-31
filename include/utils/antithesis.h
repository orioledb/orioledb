/*-------------------------------------------------------------------------
 *
 * antithesis.h
 *		OrioleDB entry point to the vendored Antithesis C SDK.
 *
 * Include this header rather than antithesis_sdk_c.h directly: it keeps the
 * decision of whether the SDK can be built in one place.  The SDK hard-errors
 * unless it is compiled by GCC or Clang for an ELF target, so anywhere else
 * (a macOS development build, say) it is compiled out to no-ops.
 * The wrapper makes not using antithesis the default, and compiles out the
 * antitehsis macros and registration.
 *
 * Define USE_ANTITHESIS_SDK to enable.
 *
 * Evven with the SDK compiled in, the assertions stay silent unless
 * /usr/lib/libvoidstar.so is present (i.e. we run inside an Antithesis test)
 * or ANTITHESIS_SDK_LOCAL_OUTPUT names an output file.  Each call site emits
 * at most one message per outcome, so an ordinary build pays a relaxed atomic
 * load per evaluation.
 *
 *-------------------------------------------------------------------------
 */
#ifndef __O_ANTITHESIS_H__
#define __O_ANTITHESIS_H__

#if !defined(USE_ANTITHESIS_SDK) || \
	!defined(__ELF__) || (!defined(__GNUC__) && !defined(__clang__))
#define NO_ANTITHESIS_SDK
#endif

/*
 * The vendored SDK is third-party code and trips a couple of warnings that
 * PostgreSQL's CFLAGS enable.  Silence them here instead of patching the
 * vendored header; -Wpragmas/-Wunknown-warning-option keep older GCC
 * and Clang quiet about the option names the other one owns.
 */
#ifdef USE_ANTITHESIS_SDK
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic ignored "-Wunknown-warning-option"
#pragma GCC diagnostic ignored "-Wdeclaration-after-statement"
#pragma GCC diagnostic ignored "-Wmissing-variable-declarations"
#endif

#include "lib/antithesis_sdk_c.h"

#ifdef USE_ANTITHESIS_SDK
#pragma GCC diagnostic pop
#endif

#endif							/* __O_ANTITHESIS_H__ */
