################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../assoc.c \
../cache.c \
../daemon.c \
../hash.c \
../items.c \
../memcached.c \
../sasl_defs.c \
../sizes.c \
../slabs.c \
../solaris_priv.c \
../stats.c \
../testapp.c \
../thread.c \
../timedrun.c \
../util.c 

OBJS += \
./assoc.o \
./cache.o \
./daemon.o \
./hash.o \
./items.o \
./memcached.o \
./sasl_defs.o \
./sizes.o \
./slabs.o \
./solaris_priv.o \
./stats.o \
./testapp.o \
./thread.o \
./timedrun.o \
./util.o 

C_DEPS += \
./assoc.d \
./cache.d \
./daemon.d \
./hash.d \
./items.d \
./memcached.d \
./sasl_defs.d \
./sizes.d \
./slabs.d \
./solaris_priv.d \
./stats.d \
./testapp.d \
./thread.d \
./timedrun.d \
./util.d 


# Each subdirectory must supply rules for building sources it contributes
%.o: ../%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


