#ifndef SIMULATION_GRID_CORE_COMPILER_EXTENSIONS_HPP
#define SIMULATION_GRID_CORE_COMPILER_EXTENSIONS_HPP

#if defined(__GNUC__) || defined(__clang__)
#if defined(__has_builtin)
#if __has_builtin(__builtin_expect)
#define likely(x) __builtin_expect(x, 1)
#define unlikely(x) __builtin_expect(x, 0)
#endif
#endif
#else 
#define likely(x) x
#define unlikely(x) x
#endif
