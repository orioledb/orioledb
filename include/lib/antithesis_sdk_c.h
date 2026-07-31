/* antithesis_sdk_c.h — the Antithesis C SDK (header-only).
 *
 * Requirements: GCC or Clang targeting an ELF platform. Assertion messages
 * must be string literals with no embedded '\0' (they are pasted into the
 * catalog record at compile time). `details_json` arguments must be NULL or a
 * valid JSON object literal, e.g. "{\"key\": 1}" — the SDK inserts them
 * verbatim. SDK calls allocate and do I/O, so they are not async-signal-safe:
 * don't put assertions in signal handlers.
 *
 * Define NO_ANTITHESIS_SDK to compile every macro and function down to a
 * no-op (no catalog section, no output, nothing to link).
 */
#ifndef ANTITHESIS_SDK_C_H
#define ANTITHESIS_SDK_C_H

#include <stdint.h>

/* splitmix64, shared by the enabled and disabled variants of
 * antithesis_get_random so their generators can't drift apart. */
#define ANTITHESIS__SPLITMIX64_GAMMA 0x9e3779b97f4a7c15ULL

static inline uint64_t
antithesis__splitmix64_mix(uint64_t x)
{
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
	x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
	return x ^ (x >> 31);
}

#ifdef NO_ANTITHESIS_SDK

/*****************************************************************************
 * DISABLED VARIANT
 *****************************************************************************/

#include <time.h>

/* Thread-local so concurrent callers don't race (the enabled variant gets the
 * same guarantee from an atomic add, but this branch can't assume GCC/Clang
 * builtins). */
#if defined(_MSC_VER)
#define ANTITHESIS__THREAD_LOCAL __declspec(thread)
#elif defined(__GNUC__)
#define ANTITHESIS__THREAD_LOCAL __thread
#elif defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
#define ANTITHESIS__THREAD_LOCAL _Thread_local
#else
#define ANTITHESIS__THREAD_LOCAL	/* no known TLS: single-threaded fallback */
#endif

/* sizeof leaves its operand unevaluated: arguments count as used (no
 * -Wunused warnings when the SDK is compiled out) but no code runs. */
#define ANTITHESIS__DISABLED2(message, details_json) \
    do { (void)sizeof((message)); (void)sizeof((details_json)); } while (0)
#define ANTITHESIS__DISABLED3(cond, message, details_json) \
    do { (void)sizeof((cond)); ANTITHESIS__DISABLED2(message, details_json); } while (0)

#define ANTITHESIS_ALWAYS(cond, message, details_json) ANTITHESIS__DISABLED3(cond, message, details_json)
#define ANTITHESIS_ALWAYS_OR_UNREACHABLE(cond, message, details_json) ANTITHESIS__DISABLED3(cond, message, details_json)
#define ANTITHESIS_SOMETIMES(cond, message, details_json) ANTITHESIS__DISABLED3(cond, message, details_json)
#define ANTITHESIS_REACHABLE(message, details_json) ANTITHESIS__DISABLED2(message, details_json)
#define ANTITHESIS_UNREACHABLE(message, details_json) ANTITHESIS__DISABLED2(message, details_json)

static inline void
antithesis_setup_complete(const char *details_json)
{
	(void) details_json;
}

static inline void
antithesis_send_event(const char *name, const char *details_json)
{
	(void) name;
	(void) details_json;
}

static inline uint64_t
antithesis_get_random(void)
{
	static ANTITHESIS__THREAD_LOCAL uint64_t state;

	if (state == 0)
	{
		state = (uint64_t) time(0) ^ (uint64_t) clock() ^ (uint64_t) (uintptr_t) &state;
	}
	state += ANTITHESIS__SPLITMIX64_GAMMA;
	return antithesis__splitmix64_mix(state);
}

#else							/* NO_ANTITHESIS_SDK */

#if !defined(__GNUC__) && !defined(__clang__)
#error "The Antithesis C SDK requires GCC or Clang (define NO_ANTITHESIS_SDK to compile it out)"
#endif

#if !defined(__ELF__)
#error "The Antithesis C SDK requires an ELF target (define NO_ANTITHESIS_SDK to compile it out)"
#endif

#include <dlfcn.h>
#include <errno.h>
#include <sched.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#define ANTITHESIS_SDK_VERSION "0.1.0"
#define ANTITHESIS_PROTOCOL_VERSION "1.1.0"

/*****************************************************************************
 * CATALOG RECORDS
 *
 * One record per assertion call site, emitted into the "antithesis_catalog"
 * section whether or not the assertion ever executes. Records hold only
 * string-literal data laid out back-to-back — no pointers — so an offline
 * extractor never has to process relocations and the format is identical
 * across architectures. See sdk/c/README.md for the exact layout.
 *****************************************************************************/

#define ANTITHESIS__CATALOG_SECTION "antithesis_catalog"
#define ANTITHESIS__CATALOG_MAGIC "ANTITHESIS_CATALOG_v1"

/* `used` stops the compiler discarding the (otherwise unreferenced) record;
 * `retain` marks the section SHF_GNU_RETAIN so the linker's --gc-sections
 * keeps it too. On toolchains without `retain`, --gc-sections needs a
 * KEEP(*(antithesis_catalog)) linker-script clause. */
#if defined(__has_attribute)
#if __has_attribute(retain)
#define ANTITHESIS__RETAIN __attribute__((retain))
#endif
#endif
#ifndef ANTITHESIS__RETAIN
#define ANTITHESIS__RETAIN
#endif

#define ANTITHESIS__STRINGIZE2(x) #x
#define ANTITHESIS__STRINGIZE(x) ANTITHESIS__STRINGIZE2(x)

/* aligned(1) keeps records contiguous across translation units so the
 * extractor can walk the section record-by-record. The trailing NUL of the
 * string literal terminates the final (line-number) field. */
#define ANTITHESIS__CATALOG_RECORD(display_type, message)                     \
    static const char antithesis__catalog_record[]                           \
        __attribute__((used, section(ANTITHESIS__CATALOG_SECTION), aligned(1))) \
        ANTITHESIS__RETAIN =                                                  \
        ANTITHESIS__CATALOG_MAGIC "\0"                                        \
        display_type "\0"                                                     \
        message "\0"                                                          \
        __FILE__ "\0"                                                         \
        ANTITHESIS__STRINGIZE(__LINE__)

/*****************************************************************************
 * INTERNAL: ASSERTION TYPES
 *
 * The X-list below is the single source for the enum and the per-type
 * properties. The display strings get their own macros (rather than a column
 * in the list) because the catalog record needs them as string literals at
 * preprocessing time — they are pasted, not looked up. The type token ties
 * everything together, so adding a type without one of its pieces is a
 * compile error rather than a silent catalog/runtime mismatch.
 *****************************************************************************/

#define ANTITHESIS__DISPLAY_ALWAYS "Always"
#define ANTITHESIS__DISPLAY_ALWAYS_OR_UNREACHABLE "AlwaysOrUnreachable"
#define ANTITHESIS__DISPLAY_SOMETIMES "Sometimes"
#define ANTITHESIS__DISPLAY_REACHABLE "Reachable"
#define ANTITHESIS__DISPLAY_UNREACHABLE "Unreachable"

/* X(type_token, assert_type_string, must_hit) */
#define ANTITHESIS__FOR_EACH_ASSERT_TYPE(X)  \
    X(ALWAYS, "always", 1)                   \
    X(ALWAYS_OR_UNREACHABLE, "always", 0)    \
    X(SOMETIMES, "sometimes", 1)             \
    X(REACHABLE, "reachability", 1)          \
    X(UNREACHABLE, "reachability", 0)

enum antithesis__assert_type
{
#define ANTITHESIS__X(token, kind, must_hit) ANTITHESIS__TYPE_##token,
	ANTITHESIS__FOR_EACH_ASSERT_TYPE(ANTITHESIS__X)
#undef ANTITHESIS__X
};

static inline const char *
antithesis__assert_type_string(enum antithesis__assert_type type)
{
	switch (type)
	{
#define ANTITHESIS__X(token, kind, must_hit) case ANTITHESIS__TYPE_##token: return kind;
			ANTITHESIS__FOR_EACH_ASSERT_TYPE(ANTITHESIS__X)
#undef ANTITHESIS__X
	}
	return "reachability";
}

static inline const char *
antithesis__display_type_string(enum antithesis__assert_type type)
{
	switch (type)
	{
#define ANTITHESIS__X(token, kind, must_hit) case ANTITHESIS__TYPE_##token: return ANTITHESIS__DISPLAY_##token;
			ANTITHESIS__FOR_EACH_ASSERT_TYPE(ANTITHESIS__X)
#undef ANTITHESIS__X
	}
	return "";
}

static inline int
antithesis__must_hit(enum antithesis__assert_type type)
{
	switch (type)
	{
#define ANTITHESIS__X(token, kind, must_hit) case ANTITHESIS__TYPE_##token: return must_hit;
			ANTITHESIS__FOR_EACH_ASSERT_TYPE(ANTITHESIS__X)
#undef ANTITHESIS__X
	}
	return 0;
}

/*****************************************************************************
 * INTERNAL: SHARED STATE
 *****************************************************************************/

#define ANTITHESIS__ERROR_PREFIX "[* antithesis-sdk-c *]"
#define ANTITHESIS__VOIDSTAR_PATH "/usr/lib/libvoidstar.so"
#define ANTITHESIS__LOCAL_OUTPUT_ENV "ANTITHESIS_SDK_LOCAL_OUTPUT"

struct antithesis__state
{
	int			status;			/* 0 = uninitialized, 1 = initializing, 2 =
								 * ready */
	void		(*fuzz_json_data) (const char *message, size_t length);
	void		(*fuzz_flush) (void);
	uint64_t	(*fuzz_get_random) (void);
	FILE	   *local_file;
	uint64_t	rng_state;
};

/* Weak so that every translation unit including this header defines it and
 * the linker collapses them to a single instance per link unit — this is what
 * makes the SDK header-only without splitting output/dedup state per TU.
 * Hidden so the dynamic linker can never merge instances across link units:
 * without it, an executable's copy preempts a directly-linked shared
 * library's, and whether state is shared would depend on the consumer's
 * visibility and link flags. */
__attribute__((weak, visibility("hidden")))
struct antithesis__state antithesis__global_state;

/*****************************************************************************
 * INTERNAL: JSON ASSEMBLY
 *****************************************************************************/

struct antithesis__sb
{
	char	   *data;
	size_t		len;
	size_t		cap;
	int			failed;
};

static inline void
antithesis__sb_reserve(struct antithesis__sb *sb, size_t extra)
{
	if (sb->failed)
	{
		return;
	}
	if (sb->len + extra + 1 <= sb->cap)
	{
		return;
	}
	size_t		cap = sb->cap ? sb->cap : 256;

	while (cap < sb->len + extra + 1)
	{
		cap *= 2;
	}
	char	   *data = (char *) realloc(sb->data, cap);

	if (data == NULL)
	{
		free(sb->data);
		sb->data = NULL;
		sb->failed = 1;
		return;
	}
	sb->data = data;
	sb->cap = cap;
}

static inline void
antithesis__sb_putn(struct antithesis__sb *sb, const char *s, size_t n)
{
	antithesis__sb_reserve(sb, n);
	if (sb->failed)
	{
		return;
	}
	memcpy(sb->data + sb->len, s, n);
	sb->len += n;
	sb->data[sb->len] = '\0';
}

static inline void
antithesis__sb_puts(struct antithesis__sb *sb, const char *s)
{
	antithesis__sb_putn(sb, s, strlen(s));
}

static inline void
antithesis__sb_put_json_string(struct antithesis__sb *sb, const char *s)
{
	static const char HEX[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
	'8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

	antithesis__sb_putn(sb, "\"", 1);
	for (; s != NULL && *s != '\0'; s++)
	{
		const unsigned char c = (unsigned char) *s;

		switch (c)
		{
			case '\t':
				antithesis__sb_putn(sb, "\\t", 2);
				break;
			case '\b':
				antithesis__sb_putn(sb, "\\b", 2);
				break;
			case '\n':
				antithesis__sb_putn(sb, "\\n", 2);
				break;
			case '\f':
				antithesis__sb_putn(sb, "\\f", 2);
				break;
			case '\r':
				antithesis__sb_putn(sb, "\\r", 2);
				break;
			case '"':
				antithesis__sb_putn(sb, "\\\"", 2);
				break;
			case '\\':
				antithesis__sb_putn(sb, "\\\\", 2);
				break;
			default:
				if (c < 0x20)
				{
					char		escape[6] = {'\\', 'u', '0', '0', HEX[(c >> 4) & 0x0F], HEX[c & 0x0F]};

					antithesis__sb_putn(sb, escape, 6);
				}
				else
				{
					antithesis__sb_putn(sb, (const char *) &c, 1);
				}
		}
	}
	antithesis__sb_putn(sb, "\"", 1);
}

static inline void
antithesis__sb_put_bool(struct antithesis__sb *sb, int value)
{
	antithesis__sb_puts(sb, value ? "true" : "false");
}

static inline void
antithesis__sb_put_long(struct antithesis__sb *sb, long value)
{
	char		tmp[32];

	snprintf(tmp, sizeof(tmp), "%ld", value);
	antithesis__sb_puts(sb, tmp);
}

static inline const char *
antithesis__details_or_empty(const char *details_json)
{
	return (details_json != NULL && details_json[0] != '\0') ? details_json : "{}";
}

/*****************************************************************************
 * INTERNAL: TRANSPORT
 *****************************************************************************/

static inline void
antithesis__output_line(struct antithesis__state *st, const char *json)
{
	if (json == NULL)
	{
		return;
	}
	if (st->fuzz_json_data != NULL)
	{
		st->fuzz_json_data(json, strlen(json));
		st->fuzz_flush();
	}
	else if (st->local_file != NULL)
	{
		fprintf(st->local_file, "%s\n", json);

		/*
		 * Unlike libvoidstar (which flushes every message), stdio would
		 * buffer until exit — flush per line so local output survives
		 * crashes.
		 */
		fflush(st->local_file);
	}
}

/* Every emitter ends the same way: send unless allocation failed, then free.
 * (On failure sb->data is already NULL — antithesis__sb_reserve freed it.) */
static inline void
antithesis__sb_send(struct antithesis__state *st, struct antithesis__sb *sb)
{
	if (!sb->failed)
	{
		antithesis__output_line(st, sb->data);
	}
	free(sb->data);
}

static inline void
antithesis__emit_version_message(struct antithesis__state *st)
{
	struct antithesis__sb sb = {NULL, 0, 0, 0};
#ifdef __VERSION__
	const char *compiler_version = __VERSION__;
#else
	const char *compiler_version = "unknown";
#endif
	antithesis__sb_puts(&sb, "{\"antithesis_sdk\":{\"language\":{\"name\":\"C\",\"version\":");
	antithesis__sb_put_json_string(&sb, compiler_version);
	antithesis__sb_puts(&sb, "},\"sdk_version\":\"" ANTITHESIS_SDK_VERSION
						"\",\"protocol_version\":\"" ANTITHESIS_PROTOCOL_VERSION "\"}}");
	antithesis__sb_send(st, &sb);
}

static inline uint64_t
antithesis__random_seed(void)
{
	uint64_t	seed = 0;
	FILE	   *urandom = fopen("/dev/urandom", "rb");

	if (urandom != NULL)
	{
		if (fread(&seed, sizeof(seed), 1, urandom) != 1)
		{
			seed = 0;
		}
		fclose(urandom);
	}
	if (seed == 0)
	{
		seed = (uint64_t) time(0) ^ (uint64_t) clock() ^ (uint64_t) (uintptr_t) &antithesis__global_state;
	}
	return seed;
}

/* dlerror() only reports the most recent dl* call (a later success clears an
 * earlier failure on glibc), so each symbol is checked — and named — the
 * moment it is looked up. */
static inline void *
antithesis__voidstar_symbol(void *lib, const char *name)
{
	void	   *sym = dlsym(lib, name);

	if (sym == NULL)
	{
		const char *err = dlerror();

		fprintf(stderr, "%s Failed to resolve symbol %s in %s: %s\n",
				ANTITHESIS__ERROR_PREFIX, name, ANTITHESIS__VOIDSTAR_PATH,
				err != NULL ? err : "unknown error");
		exit(-1);
	}
	return sym;
}

static inline void
antithesis__init_state(struct antithesis__state *st)
{
	struct stat stat_buf;

	if (stat(ANTITHESIS__VOIDSTAR_PATH, &stat_buf) == 0)
	{
		void	   *lib = dlopen(ANTITHESIS__VOIDSTAR_PATH, RTLD_NOW);

		if (lib == NULL)
		{
			/*
			 * The library exists but is unusable: failing loudly beats
			 * silently testing without SDK output (matches the C++ SDK).
			 */
			const char *err = dlerror();

			fprintf(stderr, "%s Failed to load %s: %s\n",
					ANTITHESIS__ERROR_PREFIX, ANTITHESIS__VOIDSTAR_PATH,
					err != NULL ? err : "unknown error");
			exit(-1);
		}
		st->fuzz_json_data = (void (*) (const char *, size_t))
			antithesis__voidstar_symbol(lib, "fuzz_json_data");
		st->fuzz_flush = (void (*) (void))
			antithesis__voidstar_symbol(lib, "fuzz_flush");
		st->fuzz_get_random = (uint64_t (*) (void))
			antithesis__voidstar_symbol(lib, "fuzz_get_random");
	}
	else
	{
		const char *path = getenv(ANTITHESIS__LOCAL_OUTPUT_ENV);

		if (path != NULL && path[0] != '\0')
		{
			FILE	   *file = fopen(path, "w");

			if (file == NULL)
			{
				fprintf(stderr, "%s Failed to open path %s: %s\n",
						ANTITHESIS__ERROR_PREFIX, path, strerror(errno));
			}
			else if (fchmod(fileno(file), 0644) != 0)
			{
				/*
				 * Match the C++ SDK: whatever collects the output may not
				 * share our umask, so 0644 or nothing.
				 */
				fprintf(stderr, "%s Failed to set permissions on %s: %s\n",
						ANTITHESIS__ERROR_PREFIX, path, strerror(errno));
				fclose(file);
			}
			else
			{
				st->local_file = file;
			}
		}
		st->rng_state = antithesis__random_seed();
	}
	antithesis__emit_version_message(st);
}

static inline struct antithesis__state *
antithesis__get_state(void)
{
	struct antithesis__state *st = &antithesis__global_state;

	if (__atomic_load_n(&st->status, __ATOMIC_ACQUIRE) == 2)
	{
		return st;
	}
	int			expected = 0;

	if (__atomic_compare_exchange_n(&st->status, &expected, 1, 0,
									__ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
	{
		antithesis__init_state(st);
		__atomic_store_n(&st->status, 2, __ATOMIC_RELEASE);
	}
	else
	{
		while (__atomic_load_n(&st->status, __ATOMIC_ACQUIRE) != 2)
		{
			/*
			 * Another thread is initializing. Init does real I/O (dlopen or
			 * fopen), so yield instead of burning the initializer's CPU —
			 * this loop should be unreachable anyway now that the load-time
			 * constructor below runs init before threads exist.
			 */
			sched_yield();
		}
	}
	return st;
}

/* Initialize at load time, while the process is single-threaded. This closes
 * the window where a fork() child or a signal handler could observe a
 * half-initialized state and spin forever above, and keeps the dlopen/fopen
 * cost out of whichever thread would otherwise hit the first assertion. One
 * constructor per translation unit; every one after the first is a single
 * atomic load. */
__attribute__((constructor)) static void
antithesis__init_at_load(void)
{
	(void) antithesis__get_state();
}

/*****************************************************************************
 * INTERNAL: ASSERTION EMISSION
 *****************************************************************************/

static inline void
antithesis__assert_impl(
						int *seen_flags,	/* [0] = condition-false seen, [1]
											 * = condition-true seen */
						enum antithesis__assert_type type,
						int cond,
						const char *message,
						const char *file,
						const char *function,
						long line,
						const char *details_json)
{
	int		   *seen = &seen_flags[cond ? 1 : 0];

	if (__atomic_load_n(seen, __ATOMIC_RELAXED))
	{
		return;
	}
	if (__atomic_exchange_n(seen, 1, __ATOMIC_RELAXED))
	{
		return;
	}

	struct antithesis__state *st = antithesis__get_state();
	struct antithesis__sb sb = {NULL, 0, 0, 0};

	antithesis__sb_puts(&sb, "{\"antithesis_assert\":{\"hit\":true,\"must_hit\":");
	antithesis__sb_put_bool(&sb, antithesis__must_hit(type));
	antithesis__sb_puts(&sb, ",\"assert_type\":\"");
	antithesis__sb_puts(&sb, antithesis__assert_type_string(type));
	antithesis__sb_puts(&sb, "\",\"display_type\":\"");
	antithesis__sb_puts(&sb, antithesis__display_type_string(type));
	antithesis__sb_puts(&sb, "\",\"message\":");
	antithesis__sb_put_json_string(&sb, message);
	antithesis__sb_puts(&sb, ",\"condition\":");
	antithesis__sb_put_bool(&sb, cond);
	antithesis__sb_puts(&sb, ",\"id\":");
	antithesis__sb_put_json_string(&sb, message);	/* id == message, like the
													 * other SDKs */
	antithesis__sb_puts(&sb, ",\"location\":{\"class\":\"\",\"function\":");
	antithesis__sb_put_json_string(&sb, function);
	antithesis__sb_puts(&sb, ",\"file\":");
	antithesis__sb_put_json_string(&sb, file);
	antithesis__sb_puts(&sb, ",\"begin_line\":");
	antithesis__sb_put_long(&sb, line);
	antithesis__sb_puts(&sb, ",\"begin_column\":0},\"details\":");
	antithesis__sb_puts(&sb, antithesis__details_or_empty(details_json));
	antithesis__sb_puts(&sb, "}}");
	antithesis__sb_send(st, &sb);
}

/* The do-block gives each expansion its own scope, so the record and the seen
 * flags never collide even with several assertions on one line. type_token is
 * pasted into both the enum constant and the catalog display literal, so a
 * call site cannot bake a record that disagrees with its runtime events. */
#define ANTITHESIS__ASSERT_SITE(type_token, cond, message, details_json)      \
    do {                                                                      \
        ANTITHESIS__CATALOG_RECORD(ANTITHESIS__DISPLAY_##type_token, message); \
        static int antithesis__seen_flags[2];                                 \
        antithesis__assert_impl(antithesis__seen_flags,                       \
                                ANTITHESIS__TYPE_##type_token, (cond) ? 1 : 0, \
                                (message), __FILE__, __func__, __LINE__,      \
                                (details_json));                              \
    } while (0)

/*****************************************************************************
 * PUBLIC SDK: ASSERTIONS
 *
 * `message` must be a string literal. `details_json` must be NULL or a valid
 * JSON object literal, inserted verbatim.
 *****************************************************************************/

#define ANTITHESIS_ALWAYS(cond, message, details_json) \
    ANTITHESIS__ASSERT_SITE(ALWAYS, cond, message, details_json)

#define ANTITHESIS_ALWAYS_OR_UNREACHABLE(cond, message, details_json) \
    ANTITHESIS__ASSERT_SITE(ALWAYS_OR_UNREACHABLE, cond, message, details_json)

#define ANTITHESIS_SOMETIMES(cond, message, details_json) \
    ANTITHESIS__ASSERT_SITE(SOMETIMES, cond, message, details_json)

#define ANTITHESIS_REACHABLE(message, details_json) \
    ANTITHESIS__ASSERT_SITE(REACHABLE, 1, message, details_json)

#define ANTITHESIS_UNREACHABLE(message, details_json) \
    ANTITHESIS__ASSERT_SITE(UNREACHABLE, 0, message, details_json)

/*****************************************************************************
 * PUBLIC SDK: LIFECYCLE
 *****************************************************************************/

static inline void
antithesis_setup_complete(const char *details_json)
{
	struct antithesis__state *st = antithesis__get_state();
	struct antithesis__sb sb = {NULL, 0, 0, 0};

	antithesis__sb_puts(&sb, "{\"antithesis_setup\":{\"status\":\"complete\",\"details\":");
	antithesis__sb_puts(&sb, antithesis__details_or_empty(details_json));
	antithesis__sb_puts(&sb, "}}");
	antithesis__sb_send(st, &sb);
}

static inline void
antithesis_send_event(const char *name, const char *details_json)
{
	struct antithesis__state *st = antithesis__get_state();
	struct antithesis__sb sb = {NULL, 0, 0, 0};

	antithesis__sb_puts(&sb, "{");
	antithesis__sb_put_json_string(&sb, name != NULL ? name : "");
	antithesis__sb_puts(&sb, ":");
	antithesis__sb_puts(&sb, antithesis__details_or_empty(details_json));
	antithesis__sb_puts(&sb, "}");
	antithesis__sb_send(st, &sb);
}

/*****************************************************************************
 * PUBLIC SDK: RANDOM
 *****************************************************************************/

static inline uint64_t
antithesis_get_random(void)
{
	struct antithesis__state *st = antithesis__get_state();

	if (st->fuzz_get_random != NULL)
	{
		return st->fuzz_get_random();
	}
	/* the atomic add makes concurrent callers draw distinct values */
	return antithesis__splitmix64_mix(
									  __atomic_add_fetch(&st->rng_state, ANTITHESIS__SPLITMIX64_GAMMA, __ATOMIC_RELAXED));
}

#endif							/* NO_ANTITHESIS_SDK */

/*****************************************************************************
 * OPTIONAL SHORT NAMES (C++-SDK-style, opt-in to avoid namespace pollution)
 *****************************************************************************/

#ifdef ANTITHESIS_SDK_SHORT_NAMES
#define ALWAYS(cond, message, details_json) ANTITHESIS_ALWAYS(cond, message, details_json)
#define ALWAYS_OR_UNREACHABLE(cond, message, details_json) ANTITHESIS_ALWAYS_OR_UNREACHABLE(cond, message, details_json)
#define SOMETIMES(cond, message, details_json) ANTITHESIS_SOMETIMES(cond, message, details_json)
#define REACHABLE(message, details_json) ANTITHESIS_REACHABLE(message, details_json)
#define UNREACHABLE(message, details_json) ANTITHESIS_UNREACHABLE(message, details_json)
#endif

#endif							/* ANTITHESIS_SDK_C_H */
