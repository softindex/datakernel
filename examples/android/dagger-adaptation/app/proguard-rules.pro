-verbose

-keepattributes Signature
-keepattributes InnerClasses
-keepattributes Exceptions
-keepattributes LineNumberTable
-keepattributes *Annotation*
-keepattributes EnclosingMethod

-keep class io.datakernel.android.** { *; }

-keepclassmembers class * {
    @fully.qualified.package.AnnotationType *;
}

