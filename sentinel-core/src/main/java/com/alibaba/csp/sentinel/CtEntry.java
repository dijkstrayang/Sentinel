/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel;

import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.context.ContextUtil;
import com.alibaba.csp.sentinel.context.NullContext;
import com.alibaba.csp.sentinel.node.Node;
import com.alibaba.csp.sentinel.slotchain.ProcessorSlot;
import com.alibaba.csp.sentinel.slotchain.ResourceWrapper;

/**
 * Linked entry within current context.
 *
 * @author jialiang.linjl
 * @author Eric Zhao
 */
class CtEntry extends Entry {

    /** entry的父entry，用于在同一个context上下文中，多次调用entry方法，保存entry之间的关系。 */
    protected Entry parent = null;
    /** entry的子entry，与parent相反 */
    protected Entry child = null;
    /** 插槽链 */
    protected ProcessorSlot<Object> chain;
    /** 上下文*/
    protected Context context;

    CtEntry(ResourceWrapper resourceWrapper, ProcessorSlot<Object> chain, Context context) {
        super(resourceWrapper);
        this.chain = chain;
        this.context = context;

        setUpEntryFor(context);
    }

    /**
     * 整理当前上下文中的调用链路关系
     * @param context
     */
    private void setUpEntryFor(Context context) {
        // The entry should not be associated to NullContext.
        if (context instanceof NullContext) {
            return;
        }
        // 获取当前的entry 并且赋给 parent，即表示当前entry的上游资源调用
        this.parent = context.getCurEntry();
        // 如果当前parent有值，说明在此之前有SphU.entry调用并且没有exit
        // 则把自己赋给parent的儿子，完成调用链条
        if (parent != null) {
            ((CtEntry)parent).child = this;
        }
        //将线程context的当前资源调用指向自己
        context.setCurEntry(this);
    }

    @Override
    public void exit(int count, Object... args) throws ErrorEntryFreeException {
        trueExit(count, args);
    }

    protected void exitForContext(Context context, int count, Object... args) throws ErrorEntryFreeException {
        if (context != null) {
            // Null context should exit without clean-up.
            if (context instanceof NullContext) {
                return;
            }
            if (context.getCurEntry() != this) {
                String curEntryNameInContext = context.getCurEntry() == null ? null : context.getCurEntry().getResourceWrapper().getName();
                // Clean previous call stack.
                CtEntry e = (CtEntry)context.getCurEntry();
                while (e != null) {
                    e.exit(count, args);
                    e = (CtEntry)e.parent;
                }
                String errorMessage = String.format("The order of entry exit can't be paired with the order of entry"
                    + ", current entry in context: <%s>, but expected: <%s>", curEntryNameInContext, resourceWrapper.getName());
                throw new ErrorEntryFreeException(errorMessage);
            } else {
                if (chain != null) {
                    chain.exit(context, resourceWrapper, count, args);
                }
                // Restore the call stack.
                context.setCurEntry(parent);
                if (parent != null) {
                    ((CtEntry)parent).child = null;
                }
                if (parent == null) {
                    // Default context (auto entered) will be exited automatically.
                    if (ContextUtil.isDefaultContext(context)) {
                        ContextUtil.exit();
                    }
                }
                // Clean the reference of context in current entry to avoid duplicate exit.
                clearEntryContext();
            }
        }
    }

    protected void clearEntryContext() {
        this.context = null;
    }

    @Override
    protected Entry trueExit(int count, Object... args) throws ErrorEntryFreeException {
        exitForContext(context, count, args);

        return parent;
    }

    @Override
    public Node getLastNode() {
        return parent == null ? null : parent.getCurNode();
    }
}