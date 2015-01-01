#ifndef SUPERNOVA_CORE_COMPILER_EXTENSIONS_HPP
#define SUPERNOVA_CORE_COMPILER_EXTENSIONS_HPP

#if defined(__GNUC__) || defined(__clang__)
#if defined(__has_builtin)
#if __has_builtin(__builtin_expect)
#define LIKELY_EXT(x) __builtin_expect(x, 1)
#define UNLIKELY_EXT(x) __builtin_expect(x, 0)
#endif
#endif
#endif

#if !defined(LIKELY_EXT)
#define LIKELY_EXT(x) x
#endif
#if !defined(UNLIKELY_EXT)
#define UNLIKELY_EXT(x) x
#endif

#endif
