<?php

namespace SenseiTarzan\Mongodm\Class;

enum ETypeRequest: string
{
    case STRING_CLASS = "string_class";
    case CLOSURE = "closure";

    public static function isValid(string $value): bool {
        return in_array($value, [self::CLASS, self::CLOSURE]);
    }
}
