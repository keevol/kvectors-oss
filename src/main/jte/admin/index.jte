@import io.vertx.core.json.*
@import com.keevol.kvectors.admin.vm.PageContext

@param JsonObject json
<%--@param PageContext c--%>

<%--!{var main = c.main();}--%>

@template.admin.layout.layout(title = "KVectors Dash", subtitle = "首页", json = json, content = @`



    <div class="relative flex h-screen">
<%--        <img src="/images/kvec-banner.jpg" alt="image" class="h-full w-full object-cover object-center" />--%>
        <div class="absolute top-1/3 left-10 max-w-full md:left-20">
            <div class="text-5xl font-bold italic text-white md:text-9xl ml-4 mb-3">KVectors Vector Database</div>
<%--            <div class="bg-white px-6 text-5xl font-bold text-black md:text-8xl -skew-x-[15deg]">MAKES JAVA GREAT AGAIN</div>--%>
            <div class="bg-white px-6 text-5xl font-bold text-black md:text-8xl -skew-x-[15deg] py-6">
                As easy as println("Hello Vectors.")</div>
        </div>
    </div>
    <!-- Main Content Area -->
<%--    <main class="flex-1 overflow-y-auto p-4 lg:p-6 bg-slate-800">--%>
        <!-- Stats Cards -->
<%--        <div class="grid grid-cols-2 lg:grid-cols-4 gap-4 lg:gap-6 mb-6 lg:mb-8">--%>
<%--            <div class="cyber-card overflow-hidden shadow-xl rounded-xl p-4 lg:p-6 glow-effect">--%>
<%--                <div class="flex items-center">--%>
<%--                    <div class="flex-shrink-0">--%>
<%--                        <i class="fas fa-database text-cyan-400 text-2xl lg:text-3xl"></i>--%>
<%--                    </div>--%>
<%--                    <div class="ml-3 lg:ml-5 w-0 flex-1">--%>
<%--                        <dl>--%>
<%--                            <dt class="text-xs lg:text-sm font-medium text-slate-400 truncate">向量数据库总数</dt>--%>
<%--                            <dd class="text-xl lg:text-2xl font-bold text-cyan-300">6</dd>--%>
<%--                        </dl>--%>
<%--                    </div>--%>
<%--                </div>--%>
<%--            </div>--%>

<%--            <div class="cyber-card overflow-hidden shadow-xl rounded-xl p-4 lg:p-6 glow-effect">--%>
<%--                <div class="flex items-center">--%>
<%--                    <div class="flex-shrink-0">--%>
<%--                        <i class="fas fa-list text-emerald-400 text-2xl lg:text-3xl"></i>--%>
<%--                    </div>--%>
<%--                    <div class="ml-3 lg:ml-5 w-0 flex-1">--%>
<%--                        <dl>--%>
<%--                            <dt class="text-xs lg:text-sm font-medium text-slate-400 truncate">向量总数</dt>--%>
<%--                            <dd class="text-xl lg:text-2xl font-bold text-emerald-300">8144</dd>--%>
<%--                        </dl>--%>
<%--                    </div>--%>
<%--                </div>--%>
<%--            </div>--%>

<%--            <div class="cyber-card overflow-hidden shadow-xl rounded-xl p-4 lg:p-6 glow-effect">--%>
<%--                <div class="flex items-center">--%>
<%--                    <div class="flex-shrink-0">--%>
<%--                        <i class="fas fa-chart-line text-yellow-400 text-2xl lg:text-3xl"></i>--%>
<%--                    </div>--%>
<%--                    <div class="ml-3 lg:ml-5 w-0 flex-1">--%>
<%--                        <dl>--%>
<%--                            <dt class="text-xs lg:text-sm font-medium text-slate-400 truncate">平均时延</dt>--%>
<%--                            <dd class="text-xl lg:text-2xl font-bold text-yellow-300">21 ms</dd>--%>
<%--                        </dl>--%>
<%--                    </div>--%>
<%--                </div>--%>
<%--            </div>--%>

<%--            <div class="cyber-card overflow-hidden shadow-xl rounded-xl p-4 lg:p-6 glow-effect">--%>
<%--                <div class="flex items-center">--%>
<%--                    <div class="flex-shrink-0">--%>
<%--                        <i class="fas fa-clock text-red-400 text-2xl lg:text-3xl"></i>--%>
<%--                    </div>--%>
<%--                    <div class="ml-3 lg:ml-5 w-0 flex-1">--%>
<%--                        <dl>--%>
<%--                            <dt class="text-xs lg:text-sm font-medium text-slate-400 truncate">总用户数</dt>--%>
<%--                            <dd class="text-xl lg:text-2xl font-bold text-red-300">18</dd>--%>
<%--                        </dl>--%>
<%--                    </div>--%>
<%--                </div>--%>
<%--            </div>--%>
<%--        </div>--%>


<%--        <div class="flex justify-center">--%>
<%--            <div class="text-center">--%>
<%--                <img src="/images/papercuts/owl.png" loading="lazy"/>--%>
<%--                <img src="/images/kvec-banner.jpg" loading="lazy" width="79%"  alt="kvectors logo/banner"/>--%>
<%--            </div>--%>
<%--        </div>--%>

<%--    </main>--%>







`)
