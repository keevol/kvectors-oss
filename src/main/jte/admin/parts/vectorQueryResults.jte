@import com.keevol.kvectors.VectorResult
@import io.vertx.core.json.*
@import java.util.*

@param List<VectorResult> results
@if(results == null || results.isEmpty())
    没有匹配到相似分数的向量。
@else
    <table class="min-w-full divide-y divide-blue-500/20">
        <thead class="tech-gradient">
        <tr>
            <th class="px-3 lg:px-6 py-4 text-left text-xs font-medium text-blue-300 uppercase tracking-wider whitespace-nowrap">
                Vector ID
            </th>
            <th class="px-3 lg:px-6 py-4 text-left text-xs font-medium text-blue-300 uppercase tracking-wider whitespace-nowrap">
                Relation ID
            </th>
            <th class="hidden sm:table-cell px-3 lg:px-6 py-4 text-left text-xs font-medium text-blue-300 uppercase tracking-wider whitespace-nowrap">
                Score
            </th>
            <th class="px-3 lg:px-6 py-4 text-left text-xs font-medium text-blue-300 uppercase tracking-wider w-full">
                Metadata
            </th>
            <th class="px-3 lg:px-6 py-4 text-left text-xs font-medium text-blue-300 uppercase tracking-wider whitespace-nowrap">
                Actions
            </th>
        </tr>
        </thead>
        <tbody class="bg-slate-800/50 divide-y divide-blue-500/10">
        @for(VectorResult result: results)
            <tr class="hover:bg-blue-600/10 transition-colors duration-200">
                <td class="px-3 lg:px-6 py-4 whitespace-nowrap text-sm text-cyan-300 font-mono">${result.id()}</td>
                <td class="px-3 lg:px-6 py-4 whitespace-nowrap text-sm text-slate-300">${result.meta().getString("rid")}</td>
                <td class="hidden sm:table-cell px-3 lg:px-6 py-4 whitespace-nowrap text-sm text-slate-300">
                    ${result.score().get().toString()}
                </td>
                <td class="px-3 lg:px-6 py-4 text-sm text-slate-200">
                    <div class="break-words">
                        ${result.meta().encode()}
                    </div>
                </td>
                <td class="px-3 lg:px-6 py-4 whitespace-nowrap text-sm font-medium">
                    <div class="flex space-x-2">
<%--                        <a href="edit-record.html?collection=users&id=001"--%>
<%--                           class="text-blue-400 hover:text-blue-300 transition-colors duration-200">Edit</a>--%>
<%--                        <button onclick="deleteRecord('001')"--%>
<%--                                class="text-red-400 hover:text-red-300 transition-colors duration-200">--%>
<%--                            Delete--%>
<%--                        </button>--%>
                    </div>
                </td>
            </tr>
        @endfor
        </tbody>
    </table>
@endif

