package com.keevol.kvectors.enums;

import org.apache.commons.lang3.Strings;

/**
 * <pre>
 * :::    ::: :::::::::: :::::::::: :::     :::  ::::::::  :::
 * :+:   :+:  :+:        :+:        :+:     :+: :+:    :+: :+:
 * +:+  +:+   +:+        +:+        +:+     +:+ +:+    +:+ +:+
 * +#++:++    +#++:++#   +#++:++#   +#+     +:+ +#+    +:+ +#+
 * +#+  +#+   +#+        +#+         +#+   +#+  +#+    +#+ +#+
 * #+#   #+#  #+#        #+#          #+#+#+#   #+#    #+# #+#
 * ###    ### ########## ##########     ###      ########  ##########
 * </pre>
 * <p>
 * KEEp eVOLution!
 * <p>
 *
 * @author fq@keevol.cn
 * @since 2017.5.12
 * <p>
 * Copyright 2017 © 杭州福强科技有限公司版权所有 (<a href="https://www.keevol.cn">keevol.cn</a>)
 */
public enum IndexStrategy {
    NO_INDEX, ANN;

    public static IndexStrategy of(String v) {
        if (Strings.CI.equalsAny(v, "no", "no_index", "no-index")) {
            return IndexStrategy.NO_INDEX;
        }
        if (Strings.CI.equalsAny(v, "ann")) {
            return IndexStrategy.ANN;
        }
        throw new IllegalArgumentException("unexpected value to construct Index Strategy: " + v);
    }
}



